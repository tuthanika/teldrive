package services

import (
	"context"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/auth"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/pkg/models"
	"gorm.io/gorm"
	"encoding/json"
    "reflect"
)

func (a *apiService) ScansCreate(ctx context.Context, req *api.ScanTaskCreate) (*api.ScanTask, error) {
	userID := auth.GetUser(ctx)
	logger := logging.Component("API")
	logger.Info("api.scans.create", zap.Int64("user_id", userID), zap.String("folder_id", req.FolderId.Or("")), zap.Int64("channel_id", req.ChannelId))

	folderID := req.FolderId.Or("")
	if folderID != "" && folderID != "root" && !isUUID(folderID) {
		var destRes []models.File
		if err := a.db.Raw("select * from teldrive.create_directories(?, ?)", userID, folderID).
			Scan(&destRes).Error; err == nil && len(destRes) > 0 {
			folderID = destRes[0].ID
		}
	}
	var topicID *int64
	if val, ok := req.TopicId.Get(); ok {
		if val > 0 {
			v := int64(val)
			topicID = &v
		}
	}
	schedule := strings.TrimSpace(req.Schedule.Or(""))
	status := string(req.Status.Or("waiting"))
	if status == "active" || status == "running" {
		status = "waiting"
	}
	if status == "pause" {
		status = "paused"
	}
	if schedule != "" && status == "waiting" {
		status = "scheduled"
	}

	task := &models.ScanTask{
		UserID:        userID,
		FolderID:      folderID,
		ChannelID:     req.ChannelId,
		ChannelName:   req.ChannelName.Or(""),
		TopicID:       topicID,
		TopicName:     req.TopicName.Or(""),
		SplitMode:     req.SplitMode.Or(false),
		RuleMode:      req.RuleMode.Or(false),
		RuleString:    req.RuleString.Or(""),
		RepeatEnabled: req.RepeatEnabled.Or(false),
		Schedule:      schedule,
		Status:        status,
	}

	if err := a.db.Create(task).Error; err != nil {
		return nil, err
	}

	return a.toApiScanTask(task), nil
}

func (a *apiService) ScansDelete(ctx context.Context, params api.ScansDeleteParams) error {
	userID := auth.GetUser(ctx)
	logger := logging.Component("API")
	logger.Info("api.scans.delete", zap.Int64("user_id", userID), zap.String("id", params.ID))

	// 1. Audit Check
	var totalTasks int64
	a.db.Model(&models.ScanTask{}).Count(&totalTasks)
	logger.Debug("cron.scan_tasks.audit", zap.Int64("total_db_tasks", totalTasks))

	// 2. Count Active Tasks
	if err := a.db.Where("id = ? AND user_id = ?", params.ID, userID).Delete(&models.ScanTask{}).Error; err != nil {
		return err
	}
	return nil
}

func (a *apiService) ScansList(ctx context.Context) (*api.ScanTaskList, error) {
	userID := auth.GetUser(ctx)
	var tasks []models.ScanTask
	if err := a.db.Where("user_id = ?", userID).Order("created_at desc").Find(&tasks).Error; err != nil {
		return nil, err
	}
	logger := logging.Component("API")
	logger.Info("api.scans.list", zap.Int64("user_id", userID), zap.Int("count", len(tasks)))

	res := make([]api.ScanTask, len(tasks))
	for i, task := range tasks {
		if task.FolderID != "" && task.FolderID != "root" {
			// Resolve Full Path
			var path string
			err := a.db.Raw(`
				WITH RECURSIVE path_cte AS (
					SELECT id, parent_id, name, name::text AS path_full
					FROM teldrive.files
					WHERE id = ?
					UNION ALL
					SELECT f.id, f.parent_id, f.name, f.name || '/' || p.path_full
					FROM teldrive.files f
					JOIN path_cte p ON f.id = p.parent_id
				)
				SELECT CASE 
					WHEN path_full LIKE 'root/%' THEN '/' || SUBSTRING(path_full FROM 6) 
					WHEN path_full = 'root' THEN '/'
					ELSE '/' || path_full 
				END FROM path_cte WHERE parent_id IS NULL
			`, task.FolderID).Scan(&path).Error
			if err == nil {
				task.FolderPath = path
				parts := strings.Split(path, "/")
				if len(parts) > 0 {
					task.FolderName = parts[len(parts)-1]
				}
			}
		} else {
			task.FolderPath = "/"
			task.FolderName = "My Drive"
		}

		// Resolve Channel Name (Optional/Best effort)
		if task.ChannelName == "" {
			var channelName string
			if err := a.db.Raw(`
				SELECT channel_name
				FROM teldrive.channels
				WHERE user_id = ? AND channel_id = ?
				ORDER BY selected DESC, updated_at DESC
				LIMIT 1
			`, userID, task.ChannelID).Scan(&channelName).Error; err == nil {
				task.ChannelName = strings.TrimSpace(channelName)
			}
		}

		res[i] = *a.toApiScanTask(&task)
	}
	return &api.ScanTaskList{Items: res}, nil
}

func (a *apiService) ScansUpdate(ctx context.Context, req *api.ScanTaskUpdate, params api.ScansUpdateParams) (*api.ScanTask, error) {
	userID := auth.GetUser(ctx)
	var task models.ScanTask
	if err := a.db.Where("id = ? AND user_id = ?", params.ID, userID).First(&task).Error; err != nil {
		return nil, err
	}

	if val, ok := req.FolderId.Get(); ok {
		task.FolderID = val
	}
	if val, ok := req.ChannelId.Get(); ok {
		task.ChannelID = val
	}
	if val, ok := req.TopicId.Get(); ok {
		if val > 0 {
			t := int64(val)
			task.TopicID = &t
		} else {
			task.TopicID = nil
		}
	}
	if val, ok := req.ChannelName.Get(); ok {
		task.ChannelName = val
	}
	if val, ok := req.TopicName.Get(); ok {
		task.TopicName = val
	}
	if val, ok := req.SplitMode.Get(); ok {
		task.SplitMode = val
	}
	if val, ok := req.RuleMode.Get(); ok {
		task.RuleMode = val
	}
	if val, ok := req.RuleString.Get(); ok {
		task.RuleString = val
	}
	if val, ok := req.RepeatEnabled.Get(); ok {
		task.RepeatEnabled = val
	}
	if val, ok := req.Schedule.Get(); ok {
		task.Schedule = strings.TrimSpace(val)
	}

	if val, ok := req.Status.Get(); ok {
		status := string(val)
		logger := logging.Component("API")
		logger.Info("api.scans.update_status", zap.String("id", params.ID), zap.String("new_status", status))
		if status == "clear_logs" {
			if err := a.db.Model(&task).Update("logs", gorm.Expr("'[]'::jsonb")).Error; err != nil {
				return nil, err
			}
			task.Logs = nil
			return a.toApiScanTask(&task), nil
		}
		// Map UI aliases to internal queueable statuses.
		if status == "active" || status == "running" {
			status = "waiting"
		}
		if status == "pause" {
			status = "paused"
		}
		task.Status = status
	} else if task.Schedule != "" && task.Status == "waiting" {
		task.Status = "scheduled"
	}

	if err := a.db.Save(&task).Error; err != nil {
		return nil, err
	}

	return a.toApiScanTask(&task), nil
}

func (a *apiService) toApiScanTask(task *models.ScanTask) *api.ScanTask {
	topicID := api.OptInt{}
	if task.TopicID != nil && *task.TopicID > 0 {
		topicID = api.NewOptInt(int(*task.TopicID))
	}

	res := &api.ScanTask{
		ID:            task.ID,
		FolderId:      api.NewOptString(task.FolderID),
		FolderName:    api.NewOptString(task.FolderName),
		FolderPath:    api.NewOptString(task.FolderPath),
		ChannelId:     task.ChannelID,
		ChannelName:   api.NewOptString(task.ChannelName),
		TopicId:       topicID,
		TopicName:     api.NewOptString(task.TopicName),
		SplitMode:     api.NewOptBool(task.SplitMode),
		RuleMode:      api.NewOptBool(task.RuleMode),
		RuleString:    api.NewOptString(task.RuleString),
		RepeatEnabled: api.NewOptBool(task.RepeatEnabled),
		Schedule:      api.NewOptString(task.Schedule),
		Status:        api.ScanTaskStatus(task.Status),
		ScannedCount:  api.NewOptInt(task.ScannedCount),
		ImportedCount: api.NewOptInt(task.ImportedCount),
		CreatedAt:     api.NewOptDateTime(task.CreatedAt),
		UpdatedAt:     api.NewOptDateTime(task.UpdatedAt),
	}
	setScanTaskLogs(res, task.Logs)
	return res
}

func setScanTaskLogs(dst *api.ScanTask, raw []byte) {
	if dst == nil || len(raw) == 0 {
		return
	}

	parsedLogs := []string{}
	_ = json.Unmarshal(raw, &parsedLogs)

	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return
	}
	field := v.Elem().FieldByName("Logs")
	if !field.IsValid() || !field.CanSet() {
		return
	}

	// Direct assignments by kind.
	switch field.Kind() {
	case reflect.Interface:
		field.Set(reflect.ValueOf(parsedLogs))
		return
	case reflect.String:
		field.SetString(string(raw))
		return
	case reflect.Slice:
		elem := field.Type().Elem().Kind()
		if elem == reflect.Uint8 {
			field.SetBytes(raw)
			return
		}
		if elem == reflect.String {
			field.Set(reflect.ValueOf(parsedLogs))
			return
		}
	}

	// Try optional wrapper types exposing SetTo(...)
	if field.CanAddr() {
		setTo := field.Addr().MethodByName("SetTo")
		if setTo.IsValid() && setTo.Type().NumIn() == 1 {
			argType := setTo.Type().In(0)
			switch {
			case reflect.TypeOf(parsedLogs).AssignableTo(argType):
				setTo.Call([]reflect.Value{reflect.ValueOf(parsedLogs)})
				return
			case reflect.TypeOf(string(raw)).AssignableTo(argType):
				setTo.Call([]reflect.Value{reflect.ValueOf(string(raw))})
				return
			case reflect.TypeOf(raw).AssignableTo(argType):
				setTo.Call([]reflect.Value{reflect.ValueOf(raw)})
				return
			}
		}
	}
}

func (a *apiService) ScansGetConcurrency(ctx context.Context) (*api.ScansGetConcurrencyOK, error) {
	limit := a.cnf.CronJobs.ScanConcurrency
	if limit < 1 {
		limit = 1
	}
	var entry struct {
		Value []byte
	}
	if err := a.db.Table("teldrive.kv").Select("value").Where("key = ?", "scan_concurrency").First(&entry).Error; err == nil {
		if l, err := strconv.Atoi(string(entry.Value)); err == nil && l > 0 {
			limit = l
		}
	}
	return &api.ScansGetConcurrencyOK{Limit: api.NewOptInt(limit)}, nil
}

func (a *apiService) ScansSetConcurrency(ctx context.Context, req *api.ScansSetConcurrencyReq) error {
	limit := req.Limit.Or(1)
	if limit < 1 {
		limit = 1
	}
	err := a.db.Exec("INSERT INTO teldrive.kv (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
		"scan_concurrency", strconv.Itoa(limit)).Error
	return err
}