package services

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/auth"
	"github.com/tgdrive/teldrive/internal/crypt"
	"github.com/tgdrive/teldrive/internal/hash"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/internal/pool"
	"github.com/tgdrive/teldrive/internal/tgc"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/pkg/mapper"
	"github.com/tgdrive/teldrive/pkg/models"
)

var (
	saltLength      = 32
	ErrUploadFailed = errors.New("upload failed")
)

func (a *apiService) UploadsDelete(ctx context.Context, params api.UploadsDeleteParams) error {
	if err := a.db.Where("upload_id = ?", params.ID).Delete(&models.Upload{}).Error; err != nil {
		return &api.ErrorStatusCode{StatusCode: 500, Response: api.Error{Message: err.Error(), Code: 500}}
	}
	return nil
}

func (a *apiService) UploadsPartsById(ctx context.Context, params api.UploadsPartsByIdParams) ([]api.UploadPart, error) {
	parts := []models.Upload{}
	if err := a.db.Model(&models.Upload{}).Order("part_no").Where("upload_id = ?", params.ID).
		Where("created_at < ?", time.Now().UTC().Add(a.cnf.TG.Uploads.Retention)).
		Find(&parts).Error; err != nil {
		return nil, &apiError{err: err}
	}
	return mapper.ToUploadOut(parts), nil
}

func (a *apiService) UploadsStats(ctx context.Context, params api.UploadsStatsParams) ([]api.UploadStats, error) {
	userId := auth.GetUser(ctx)
	var stats []api.UploadStats
	err := a.db.Raw(`
    SELECT
    dates.upload_date::date AS upload_date,
    COALESCE(SUM(files.size), 0)::bigint AS total_uploaded
    FROM
        generate_series(
            (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date - INTERVAL '1 day' * @days,
            (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date,
            '1 day'
        ) AS dates(upload_date)
    LEFT JOIN
    teldrive.files AS files
    ON
        dates.upload_date = DATE_TRUNC('day', files.created_at)::date
        AND files.type = 'file'
        AND files.user_id = @userId
    GROUP BY
        dates.upload_date
    ORDER BY
        dates.upload_date
  `, sql.Named("days", params.Days-1), sql.Named("userId", userId)).Scan(&stats).Error

	if err != nil {
		return nil, &apiError{err: err}

	}
	return stats, nil
}

func (a *apiService) prepareEncryption(params *api.UploadsUploadParams, fileStream io.Reader, fileSize int64, logger *zap.Logger) (io.Reader, int64, string, error) {
	if !params.Encrypted.Value {
		return fileStream, fileSize, "", nil
	}
	salt, err := generateRandomSalt()
	if err != nil {
		return nil, 0, "", err
	}
	cipher, err := crypt.NewCipher(a.cnf.TG.Uploads.EncryptionKey, salt)
	if err != nil {
		return nil, 0, "", err
	}
	fileSize = crypt.EncryptedSize(fileSize)
	fileStream, err = cipher.EncryptData(fileStream)
	if err != nil {
		return nil, 0, "", err
	}
	return fileStream, fileSize, salt, nil
}

func (a *apiService) getUploadClient(ctx context.Context, userId int64) (*telegram.Client, string, int, string, error) {
	tokens, err := a.channelManager.BotTokens(ctx, userId)
	if err != nil {
		return nil, "", 0, "", err
	}

	if len(tokens) == 0 {
		client, err := tgc.AuthClient(ctx, &a.cnf.TG, auth.GetJWTUser(ctx).TgSession)
		if err != nil {
			return nil, "", 0, "", err
		}
		return client, "", 0, strconv.FormatInt(userId, 10), nil
	}

	token, index, err := a.botSelector.Next(ctx, tgc.BotOpUpload, userId, tokens)
	if err != nil {
		return nil, "", 0, "", err
	}
	client, err := tgc.BotClient(ctx, a.db, a.cache, &a.cnf.TG, token)
	if err != nil {
		return nil, "", 0, "", err
	}
	return client, token, index, strings.Split(token, ":")[0], nil
}

func (a *apiService) uploadToTelegram(ctx context.Context, client *tg.Client, channelId int64, params *api.UploadsUploadParams, fileStream io.Reader, fileSize int64, logger *zap.Logger) (*tg.Message, error) {
	channel, err := tgc.GetChannelById(ctx, client, channelId)
	if err != nil {
		return nil, err
	}

	u := uploader.NewUploader(client).WithThreads(a.cnf.TG.Uploads.Threads).WithPartSize(512 * 1024)
	upload, err := u.Upload(ctx, uploader.NewUpload(params.PartName, fileStream, fileSize))
	if err != nil {
		return nil, err
	}

	docBuilder := message.UploadedDocument(upload).Filename(params.PartName)

	uploadAsMedia := a.cnf.TG.Uploads.UploadAsMedia
	if params.UploadAsMedia.Set {
		uploadAsMedia = params.UploadAsMedia.Value
	}

	if uploadAsMedia {
		ext := strings.ToLower(params.PartName)
		if strings.HasSuffix(ext, ".mp4") || strings.HasSuffix(ext, ".mkv") || strings.HasSuffix(ext, ".mov") {
			docBuilder = docBuilder.MIME("video/mp4").Attributes(&tg.DocumentAttributeVideo{
				SupportsStreaming: true,
				W: 1280,
				H: 720,
			})
		} else if strings.HasSuffix(ext, ".mp3") || strings.HasSuffix(ext, ".m4a") || strings.HasSuffix(ext, ".flac") || strings.HasSuffix(ext, ".ogg") {
			docBuilder = docBuilder.MIME("audio/mpeg").Attributes(&tg.DocumentAttributeAudio{})
		} else if strings.HasSuffix(ext, ".jpg") || strings.HasSuffix(ext, ".jpeg") || strings.HasSuffix(ext, ".png") || strings.HasSuffix(ext, ".webp") || strings.HasSuffix(ext, ".gif") {
			docBuilder = docBuilder.MIME("image/jpeg").Attributes(&tg.DocumentAttributeImageSize{
				W: 1280,
				H: 720,
			})
		} else {
			docBuilder = docBuilder.ForceFile(true)
		}
	} else {
		docBuilder = docBuilder.ForceFile(true)
	}

	document := docBuilder
	peer := &tg.InputPeerChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash}

	var res tg.UpdatesClass
	if params.TopicId.Set && params.TopicId.Value != 0 {
		// Send to a specific forum topic using raw API with reply_to.
		// Build InputMediaUploadedDocument manually so we can use MessagesSendMedia.
		var randID int64
		if err = binary.Read(rand.Reader, binary.LittleEndian, &randID); err != nil {
			return nil, err
		}
		// Build MIME type and attributes same as docBuilder logic
		topicMIME := "application/octet-stream"
		topicAttrs := []tg.DocumentAttributeClass{
			&tg.DocumentAttributeFilename{FileName: params.PartName},
		}
		ext := strings.ToLower(params.PartName)
		if uploadAsMedia {
			if strings.HasSuffix(ext, ".mp4") || strings.HasSuffix(ext, ".mkv") || strings.HasSuffix(ext, ".mov") {
				topicMIME = "video/mp4"
				topicAttrs = append(topicAttrs, &tg.DocumentAttributeVideo{SupportsStreaming: true, W: 1280, H: 720})
			} else if strings.HasSuffix(ext, ".mp3") || strings.HasSuffix(ext, ".m4a") || strings.HasSuffix(ext, ".flac") || strings.HasSuffix(ext, ".ogg") {
				topicMIME = "audio/mpeg"
				topicAttrs = append(topicAttrs, &tg.DocumentAttributeAudio{})
			} else if strings.HasSuffix(ext, ".jpg") || strings.HasSuffix(ext, ".jpeg") || strings.HasSuffix(ext, ".png") || strings.HasSuffix(ext, ".webp") || strings.HasSuffix(ext, ".gif") {
				topicMIME = "image/jpeg"
				topicAttrs = append(topicAttrs, &tg.DocumentAttributeImageSize{W: 1280, H: 720})
			}
		}
		mediaDoc := &tg.InputMediaUploadedDocument{
			File:       upload,
			MimeType:   topicMIME,
			Attributes: topicAttrs,
		}
		res, err = client.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
			Peer:     peer,
			Media:    mediaDoc,
			ReplyTo:  &tg.InputReplyToMessage{ReplyToMsgID: params.TopicId.Value},
			RandomID: randID,
		})
	} else {
		sender := message.NewSender(client)
		res, err = sender.To(peer).Media(ctx, document)
	}
	if err != nil {
		return nil, err
	}

	updates := res.(*tg.Updates)
	var message *tg.Message
	for _, update := range updates.Updates {
		if channelMsg, ok := update.(*tg.UpdateNewChannelMessage); ok {
			if msg, ok := channelMsg.Message.AsNotEmpty(); ok {
				if m, ok := msg.(*tg.Message); ok {
					message = m
					break
				}
			}
		}
	}

	if message == nil || message.ID == 0 {
		return nil, fmt.Errorf("upload failed: invalid message ID 0 from telegram")
	}
	return message, nil
}

func (a *apiService) UploadsUpload(ctx context.Context, req *api.UploadsUploadReqWithContentType, params api.UploadsUploadParams) (*api.UploadPart, error) {
	if params.Encrypted.Value && a.cnf.TG.Uploads.EncryptionKey == "" {
		return nil, &apiError{err: errors.New("encryption is not enabled"), code: 400}
	}

	userId := auth.GetUser(ctx)
	// Create upload component logger with common fields
	logger := logging.Component("UPLOAD").With(
		zap.String("file_name", params.FileName),
		zap.String("part_name", params.PartName),
		zap.Int("part_no", params.PartNo),
		zap.Int64("size", params.ContentLength),
	)

	channelId := params.ChannelId.Value
	var (
		err         error
		parentIdPtr *string
	)
	if params.Path.Value != "" {
		parentIdPtr, err = resolvePathID(a.db, params.Path.Value, userId)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &apiError{err: err}
		}
	}
	if channelId == 0 {
		channelId, err = ResolveChannelID(a.db, parentIdPtr)
		if err != nil {
			return nil, &apiError{err: err}
		}
		if channelId == 0 {
			channelId, err = a.channelManager.CurrentChannel(ctx, userId)
			if err != nil && !errors.Is(err, tgc.ErrNoDefaultChannel) {
				return nil, &apiError{err: err}
			}
			if err == tgc.ErrNoDefaultChannel || (a.cnf.TG.AutoChannelCreate && a.channelManager.ChannelLimitReached(channelId)) {
				newChannelId, err := a.channelManager.CreateNewChannel(ctx, "", userId, true)
				if err != nil {
					logger.Error("channel.create.failed", zap.Error(err))
					return nil, &apiError{err: err}
				}
				channelId = newChannelId
				logger.Debug("channel.created", zap.Int64("new_channel_id", channelId))
			}
		}
	}

	topicId := params.TopicId.Value
	if topicId == 0 && params.ChannelId.Value == 0 {
		topicId, err = ResolveTopicID(a.db, parentIdPtr)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &apiError{err: err}
		}
		if topicId != 0 {
			params.TopicId = api.NewOptInt(topicId)
		} else {
			// Try to resolve topicId if we resolved channelId from DB
			var dbFile models.File
			if err := a.db.Where("user_id = ?", userId).Where("channel_id = ?", channelId).Where("topic_id IS NOT NULL").Order("updated_at DESC").First(&dbFile).Error; err == nil {
				if dbFile.TopicId != nil {
					topicId = *dbFile.TopicId
					params.TopicId = api.NewOptInt(topicId)
				}
			}
		}
	}

	client, token, index, channelUser, err := a.getUploadClient(ctx, userId)
	if err != nil {
		return nil, &apiError{err: err}
	}

	logger.Debug("upload.started", zap.String("bot", channelUser), zap.Int("bot_no", index), zap.Int64("size", params.ContentLength))

	uploadPool := pool.NewPool(client, int64(a.cnf.TG.PoolSize), a.newMiddlewares(ctx, a.cnf.TG.Uploads.MaxRetries)...)
	defer func() { uploadPool.Close() }()

	var out api.UploadPart
	// Compute BLAKE3 block hashes on plaintext BEFORE encryption
	var blockHasher *hash.BlockHasher
	var reader io.Reader = req.Content.Data

	if params.Hashing.Value {
		blockHasher = hash.NewBlockHasher()
		reader = io.TeeReader(req.Content.Data, blockHasher)
	}

	err = tgc.RunWithAuth(ctx, client, token, func(ctx context.Context) error {

		client := uploadPool.Default(ctx)

		fileStream, fileSize, salt, err := a.prepareEncryption(&params, reader, params.ContentLength, logger)
		if err != nil {
			return err
		}

		message, err := a.uploadToTelegram(ctx, client, channelId, &params, fileStream, fileSize, logger)

		if err != nil {
			return err
		}

		doc, ok := msgDocument(message)

		if !ok || (doc.Size == 0 && doc.Size != fileSize) {
			return ErrUploadFailed
		}

		var blockHashes []byte
		if blockHasher != nil {
			blockHashes = blockHasher.Sum()
		}

		partUpload := &models.Upload{
			Name:        params.PartName,
			UploadId:    params.ID,
			PartId:      message.ID,
			ChannelId:   channelId,
			Size:        fileSize,
			PartNo:      params.PartNo,
			UserId:      userId,
			Encrypted:   params.Encrypted.Value,
			Salt:        salt,
			BlockHashes: blockHashes,
		}

		if err := a.db.Create(partUpload).Error; err != nil {
			return err
		}

		out = api.UploadPart{
			Name:      partUpload.Name,
			PartId:    partUpload.PartId,
			ChannelId: partUpload.ChannelId,
			PartNo:    partUpload.PartNo,
			Size:      partUpload.Size,
			Encrypted: partUpload.Encrypted,
		}
		out.SetSalt(api.NewOptString(partUpload.Salt))
		return nil
	})

	if err != nil {
		logger.Error("upload.failed", zap.String("file_name", params.FileName),
			zap.String("part_name", params.PartName),
			zap.Int("part_no", params.PartNo), zap.Error(err))
		return nil, &apiError{err: err}
	}
	logger.Debug("upload.complete", zap.Int("message_id", out.PartId), zap.Int64("final_size", out.Size), zap.Bool("encrypted", out.Encrypted))
	return &out, nil
}

func msgDocument(m tg.MessageClass) (*tg.Document, bool) {
	res, ok := m.AsNotEmpty()
	if !ok {
		return nil, false
	}
	msg, ok := res.(*tg.Message)
	if !ok {
		return nil, false
	}

	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok || media == nil {
		return nil, false
	}
	doc, ok := media.Document.AsNotEmpty()
	if !ok {
		return nil, false
	}
	return doc, true
}

func generateRandomSalt() (string, error) {
	randomBytes := make([]byte, saltLength)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", err
	}

	hasher := sha256.New()
	hasher.Write(randomBytes)
	hashedSalt := base64.URLEncoding.EncodeToString(hasher.Sum(nil))

	return hashedSalt, nil
}