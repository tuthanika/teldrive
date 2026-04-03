package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/tgc"
	"github.com/tgdrive/teldrive/pkg/models"
	"go.uber.org/zap"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type scanTaskWorker struct {
	db     *gorm.DB
	cnf    *config.ServerCmdConfig
	logger *zap.Logger
}

func NewScanTaskWorker(db *gorm.DB, cnf *config.ServerCmdConfig, logger *zap.Logger) *scanTaskWorker {
	return &scanTaskWorker{db: db, cnf: cnf, logger: logger}
}

func (w *scanTaskWorker) ProcessTask(ctx context.Context, taskID string) error {
	var task models.ScanTask
	if err := w.db.First(&task, "id = ?", taskID).Error; err != nil {
		return err
	}

	w.logger.Info("starting scan task", zap.String("id", task.ID), zap.Int64("user_id", task.UserID))
	task.Status = "running"
	task.ScannedCount = 0
	task.ImportedCount = 0
	task.Logs = datatypes.JSON([]byte("[]"))
	w.db.Model(&task).Updates(map[string]interface{}{
		"status":         "running",
		"scanned_count":  0,
		"imported_count": 0,
		"logs":           task.Logs,
	})
	w.addTaskLog(&task, "task started")

	// Get latest session for user
	var session models.Session
	if err := w.db.Where("user_id = ?", task.UserID).Order("created_at desc").First(&session).Error; err != nil {
		w.markError(&task, "no active session found for user")
		return err
	}

	middlewares := tgc.NewMiddleware(&w.cnf.TG, tgc.WithFloodWait(), tgc.WithRateLimit())
	client, err := tgc.AuthClient(ctx, &w.cnf.TG, session.Session, middlewares...)
	if err != nil {
		w.markError(&task, fmt.Sprintf("failed to auth client: %v", err))
		return err
	}
	w.logger.Info("scan task telegram client initialized", zap.String("id", task.ID))

	if err := tgc.RunWithAuth(ctx, client, "", func(runCtx context.Context) error {
		w.logger.Info("scan task telegram client authorized", zap.String("id", task.ID))
		apiClient := tg.NewClient(client)

		// Resolve peer and update metadata
		peer, name, err := w.resolvePeer(runCtx, apiClient, task.ChannelID)
		if err != nil {
			return fmt.Errorf("failed to resolve peer: %w", err)
		}

		if name != "" && task.ChannelName == "" {
			task.ChannelName = name
			w.db.Model(&task).Update("channel_name", name)
		}

		w.logger.Info("resolved channel metadata", zap.String("name", name), zap.Int64("id", task.ChannelID))
		w.addTaskLog(&task, fmt.Sprintf("channel resolved: %s (%d)", name, task.ChannelID))

		offsetID := 0
		for {
			// Check for cancellation or status changes midpoint
			var currentTask models.ScanTask
			if err := w.db.Select("status").First(&currentTask, "id = ?", task.ID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					w.logger.Info("scan task stopping as it was deleted", zap.String("id", task.ID))
					return nil
				}
			} else if currentTask.Status != "running" {
				w.logger.Info("scan task stopping as status is no longer running", zap.String("id", task.ID), zap.String("status", currentTask.Status))
				return nil
			}

			var messages []tg.MessageClass

			if task.TopicID != nil {
				res, err := apiClient.MessagesGetReplies(runCtx, &tg.MessagesGetRepliesRequest{
					Peer:     peer,
					MsgID:    int(*task.TopicID),
					Limit:    100,
					OffsetID: offsetID,
				})
				if err != nil {
					return fmt.Errorf("failed to get replies for topic: %w", err)
				}
				switch m := res.(type) {
				case *tg.MessagesMessages:
					messages = m.Messages
				case *tg.MessagesMessagesSlice:
					messages = m.Messages
				case *tg.MessagesChannelMessages:
					messages = m.Messages
				}
			} else {
				res, err := apiClient.MessagesGetHistory(runCtx, &tg.MessagesGetHistoryRequest{
					Peer:     peer,
					Limit:    100,
					OffsetID: offsetID,
				})
				if err != nil {
					return fmt.Errorf("failed to get history: %w", err)
				}
				switch m := res.(type) {
				case *tg.MessagesMessages:
					messages = m.Messages
				case *tg.MessagesMessagesSlice:
					messages = m.Messages
				case *tg.MessagesChannelMessages:
					messages = m.Messages
				}
			}

			if len(messages) == 0 {
				w.logger.Info("no more messages found", zap.String("id", task.ID))
				break
			}

			w.logger.Info("processing message batch",
				zap.Int("count", len(messages)),
				zap.Int("first_id", messages[0].GetID()),
				zap.Int("last_id", messages[len(messages)-1].GetID()),
				zap.Int("current_offset", offsetID),
			)

			imported := 0
			lastID := offsetID
			for _, rawMsg := range messages {
				lastID = rawMsg.GetID()
				msg, ok := rawMsg.(*tg.Message)
				if !ok || msg.Media == nil {
					continue
				}
				count, detail := w.processMedia(runCtx, &task, msg, msg.Media)
				imported += count
				if detail != "" {
					w.addTaskLog(&task, detail)
				}
			}

			// Update progress in database incrementally
			w.db.Model(&models.ScanTask{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
				"scanned_count":  gorm.Expr("scanned_count + ?", len(messages)),
				"imported_count": gorm.Expr("imported_count + ?", imported),
			})
			w.addTaskLog(&task, fmt.Sprintf("batch scanned=%d imported=%d first_id=%d last_id=%d", len(messages), imported, messages[0].GetID(), messages[len(messages)-1].GetID()))

			if len(messages) < 100 || lastID == offsetID {
				w.logger.Info("reached end of message history", zap.Int("last_id", lastID))
				break
			}
			offsetID = lastID
		}
		return nil
	}); err != nil {
		w.markError(&task, err.Error())
		return err
	}

	w.db.Model(&models.ScanTask{}).Where("id = ?", task.ID).Update("status", "completed")
	w.addTaskLog(&task, "task completed successfully")
	w.logger.Info("scan task completed successfully", zap.String("id", task.ID))
	return nil
}

func (w *scanTaskWorker) processMedia(ctx context.Context, task *models.ScanTask, msg *tg.Message, media tg.MessageMediaClass) (int, string) {
	var fileName string
	var mimeType string
	var size int64
	var docID int64

	switch m := media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := m.Document.(*tg.Document)
		if !ok {
			return 0, ""
		}
		docID = doc.ID
		mimeType = doc.MimeType
		size = doc.Size
		for _, attr := range doc.Attributes {
			if fileAttr, ok := attr.(*tg.DocumentAttributeFilename); ok {
				fileName = fileAttr.FileName
				break
			}
		}
	case *tg.MessageMediaPhoto:
		photo, ok := m.Photo.(*tg.Photo)
		if !ok {
			return 0, ""
		}
		docID = photo.ID
		mimeType = "image/jpeg"
		for _, sz := range photo.Sizes {
			switch s := sz.(type) {
			case *tg.PhotoSize:
				if int64(s.Size) > size {
					size = int64(s.Size)
				}
			case *tg.PhotoSizeProgressive:
				// Skip progressive for size calculation as W*H is not bytes
			case *tg.PhotoCachedSize:
				if int64(len(s.Bytes)) > size {
					size = int64(len(s.Bytes))
				}
			}
		}
		fileName = fmt.Sprintf("photo_%d.jpg", msg.ID)
	default:
		return 0, ""
	}

	if fileName == "" {
		fileName = fmt.Sprintf("file_%d", docID)
	}

	// Check if already exists
	var count int64
	w.db.Model(&models.File{}).Where("channel_id = ? AND parts @> ?", task.ChannelID, fmt.Sprintf("[{\"id\":%d}]", msg.ID)).Count(&count)
	if count > 0 {
		return 0, fmt.Sprintf("skip duplicate message: msg_id=%d channel_id=%d link=%s", msg.ID, task.ChannelID, messageLink(task.ChannelID, msg.ID))
	}

	// Resolve target parent
	targetParentID := w.resolveFolder(ctx, task.UserID, task.FolderID)

	// Apply mapping rules if enabled
	if task.RuleMode && task.RuleString != "" {
		rules := strings.Split(task.RuleString, ",")
		for _, rule := range rules {
			parts := strings.Split(strings.TrimSpace(rule), ":")
			if len(parts) == 2 {
				ext := strings.TrimPrefix(strings.ToLower(parts[0]), ".")
				subfolderName := strings.TrimSpace(parts[1])
				if strings.HasSuffix(strings.ToLower(fileName), "."+ext) {
					targetParentID = w.ensureSubfolder(ctx, task.UserID, targetParentID, subfolderName)
					break
				}
			}
		}
	} else if task.SplitMode {
		cat := w.getCategory(fileName, mimeType)
		targetParentID = w.ensureSubfolder(ctx, task.UserID, targetParentID, cat)
	}

	var parentPtr *string
	if targetParentID != "" && targetParentID != "root" {
		parentPtr = &targetParentID
	}

	fileID := uuid.New().String()
	now := time.Now().UTC()
	var topicPtr *int
	if task.TopicID != nil {
		t := int(*task.TopicID)
		topicPtr = &t
	}
	parts := datatypes.JSONSlice[api.Part]{{ID: msg.ID}}

	cat := w.getCategory(fileName, mimeType)

	newFile := &models.File{
		ID:        fileID,
		Name:      fileName,
		Type:      "file",
		MimeType:  mimeType,
		Size:      &size,
		Category:  &cat,
		ChannelId: &task.ChannelID,
		Parts:     &parts,
		ParentId:  parentPtr,
		UserId:    task.UserID,
		Status:    "active",
		TopicId:   topicPtr,
		CreatedAt: &now,
		UpdatedAt: &now,
	}

	if err := w.db.Create(newFile).Error; err != nil {
		w.logger.Error("failed to create file record", zap.Error(err))
		return 0, fmt.Sprintf("failed importing file: %s (msg_id=%d parent_id=%s mime=%s size=%d link=%s): %v", fileName, msg.ID, targetParentID, mimeType, size, messageLink(task.ChannelID, msg.ID), err)
	}
	return 1, fmt.Sprintf("imported file: %s (msg_id=%d file_id=%s parent_id=%s mime=%s size=%d link=%s)", fileName, msg.ID, fileID, targetParentID, mimeType, size, messageLink(task.ChannelID, msg.ID))
}

func messageLink(channelID int64, messageID int) string {
	internalID := channelID
	if internalID > 1000000000000 {
		internalID = internalID - 1000000000000
	}
	if internalID < 0 {
		internalID = -internalID
	}
	return fmt.Sprintf("https://t.me/c/%d/%d", internalID, messageID)
}

func (w *scanTaskWorker) resolveFolder(ctx context.Context, userID int64, folderIDOrName string) string {
	if folderIDOrName == "" || folderIDOrName == "root" {
		return ""
	}

	// check if it's a UUID
	if _, err := uuid.Parse(folderIDOrName); err == nil {
		return folderIDOrName
	}

	// treat as name, find or create in root
	return w.ensureSubfolder(ctx, userID, "", folderIDOrName)
}

func (w *scanTaskWorker) resolvePeer(ctx context.Context, apiClient *tg.Client, channelID int64) (tg.InputPeerClass, string, error) {
	// Try to find the channel in user's dialogs to get access hash
	limit := 100
	res, err := apiClient.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
		Limit:      limit,
		OffsetDate: 0,
		OffsetID:   0,
		OffsetPeer: &tg.InputPeerEmpty{},
	})
	if err != nil {
		return nil, "", err
	}

	var chats []tg.ChatClass
	switch m := res.(type) {
	case *tg.MessagesDialogs:
		chats = m.Chats
	case *tg.MessagesDialogsSlice:
		chats = m.Chats
	}

	for _, chat := range chats {
		if ch, ok := chat.(*tg.Channel); ok {
			// Match ID (considering potential prefix differences)
			if ch.ID == channelID || ch.ID == channelID-1000000000000 || ch.ID*-1 == channelID {
				return &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash}, ch.Title, nil
			}
		}
		if ch, ok := chat.(*tg.Chat); ok {
			if ch.ID == channelID {
				return &tg.InputPeerChat{ChatID: ch.ID}, ch.Title, nil
			}
		}
	}

	// Fallback to minimal peer if not found in dialogs (might fail for some operations)
	return &tg.InputPeerChannel{ChannelID: channelID}, "", nil
}

func (w *scanTaskWorker) getCategory(fileName, mime string) string {
	mime = strings.ToLower(mime)
	if strings.HasPrefix(mime, "image/") {
		return "img"
	}
	if strings.HasPrefix(mime, "video/") {
		return "video"
	}
	if strings.HasPrefix(mime, "audio/") {
		return "audio"
	}
	if strings.HasPrefix(mime, "text/") || mime == "application/pdf" {
		return "ebook"
	}
	return "other"
}

func (w *scanTaskWorker) ensureSubfolder(ctx context.Context, userID int64, parentID, folderName string) string {
	var f models.File
	var query *gorm.DB
	if parentID == "root" || parentID == "" {
		query = w.db.Where("user_id = ? AND parent_id IS NULL AND name = ? AND type = 'folder'", userID, folderName)
	} else {
		query = w.db.Where("user_id = ? AND parent_id = ? AND name = ? AND type = 'folder'", userID, parentID, folderName)
	}

	err := query.First(&f).Error
	if err == nil {
		return f.ID
	}

	fID := uuid.New().String()
	var parentPtr *string
	if parentID != "" && parentID != "root" {
		parentPtr = &parentID
	}

	now := time.Now().UTC()
	f = models.File{
		ID:        fID,
		Name:      folderName,
		Type:      "folder",
		MimeType:  "drive/folder",
		ParentId:  parentPtr,
		UserId:    userID,
		Status:    "active",
		CreatedAt: &now,
		UpdatedAt: &now,
	}
	if err := w.db.Create(&f).Error; err != nil {
		w.logger.Error("failed to create folder", zap.Error(err))
		return ""
	}
	return fID
}

func (w *scanTaskWorker) markError(task *models.ScanTask, errorMsg string) {
	task.Status = "cancelled"
	w.appendLog(task, errorMsg)
	w.db.Model(task).Updates(map[string]interface{}{
		"status": "cancelled",
		"logs":   task.Logs,
	})
}

func (w *scanTaskWorker) appendLog(task *models.ScanTask, entry string) {
	var logs []string
	if len(task.Logs) > 0 {
		_ = json.Unmarshal(task.Logs, &logs)
	}
	timestamp := time.Now().Format(time.RFC3339)
	logs = append(logs, fmt.Sprintf("[%s] %s", timestamp, entry))
	if len(logs) > 100 {
		logs = logs[len(logs)-100:]
	}
	b, _ := json.Marshal(logs)
	task.Logs = datatypes.JSON(b)
}

func (w *scanTaskWorker) addTaskLog(task *models.ScanTask, entry string) {
	w.appendLog(task, entry)
	w.db.Model(task).Update("logs", task.Logs)
}

