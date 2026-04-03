package cron

import (
	"context"
	"strconv"
    "strings"
	"time"

	gormlock "github.com/go-co-op/gocron-gorm-lock/v2"
	"github.com/go-co-op/gocron/v2"
	"github.com/jackc/pgx/v5/pgtype"
	rcron "github.com/robfig/cron/v3"
	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/internal/tgc"
	"github.com/tgdrive/teldrive/pkg/models"
	"github.com/tgdrive/teldrive/pkg/services"
	"go.uber.org/zap"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type file struct {
	ID    string     `json:"id"`
	Parts []api.Part `json:"parts"`
}

type result struct {
	Files     datatypes.JSONSlice[file]
	ChannelId int64
	UserId    int64
	Session   string
}

type uploadResult struct {
	Parts     datatypes.JSONSlice[int]
	Session   string
	UserId    int64
	ChannelId int64
}

type CronService struct {
	db     *gorm.DB
	cnf    *config.ServerCmdConfig
	logger *zap.Logger
}

func StartCronJobs(ctx context.Context, db *gorm.DB, cnf *config.ServerCmdConfig) error {

	err := db.AutoMigrate(&gormlock.CronJobLock{})
	if err != nil {
		return err
	}

	locker, err := gormlock.NewGormLocker(db, cnf.CronJobs.LockerInstance,
		gormlock.WithCleanInterval(time.Hour*12))

	if err != nil {
		return err
	}

	scheduler, err := gocron.NewScheduler(gocron.WithLocation(time.UTC),
		gocron.WithDistributedLocker(locker))

	if err != nil {
		return err
	}

	cron := CronService{db: db, cnf: cnf, logger: logging.Component("CRON")}
	_, err = scheduler.NewJob(gocron.DurationJob(cnf.CronJobs.CleanFilesInterval),
		gocron.NewTask(cron.cleanFiles, ctx))
	if err != nil {
		return err
	}
	_, err = scheduler.NewJob(gocron.DurationJob(cnf.CronJobs.FolderSizeInterval),
		gocron.NewTask(cron.updateFolderSize))
	if err != nil {
		return err
	}
	_, err = scheduler.NewJob(gocron.DurationJob(cnf.CronJobs.CleanUploadsInterval),
		gocron.NewTask(cron.cleanUploads, ctx))
	if err != nil {
		return err
	}
	_, err = scheduler.NewJob(gocron.DurationJob(time.Hour*12),
		gocron.NewTask(cron.cleanOldEvents))
	if err != nil {
		return err
	}
	_, err = scheduler.NewJob(gocron.DurationJob(time.Minute),
		gocron.NewTask(cron.scanTasks, ctx))
	if err != nil {
		return err
	}

	cron.ResetTasks()
	scheduler.Start()
	return nil
}

func (c *CronService) ResetTasks() {
	c.logger.Info("cron.reset_tasks.started")
	c.db.Model(&models.ScanTask{}).Where("status = ?", "running").Update("status", "waiting")
}

func (c *CronService) cleanFiles(ctx context.Context) {
	c.logger.Info("cron.clean_files.started")
	var results []result
	if err := c.db.Table("teldrive.files as f").
		Select("JSONB_AGG(jsonb_build_object('id', f.id, 'parts', f.parts)) as files,f.channel_id,f.user_id,s.session").
		Joins("LEFT JOIN teldrive.users as u ON u.user_id = f.user_id").
		Joins(`LEFT JOIN (
        SELECT user_id, session
        FROM teldrive.sessions
        WHERE created_at = (
            SELECT MAX(created_at)
            FROM teldrive.sessions s2
            WHERE s2.user_id = sessions.user_id
        )
    ) as s ON u.user_id = s.user_id`).
		Where("f.type = ?", "file").
		Where("f.status = ?", "pending_deletion").
		Group("f.channel_id").
		Group("f.user_id").
		Group("s.session").
		Scan(&results).Error; err != nil {
		return
	}

	middlewares := tgc.NewMiddleware(&c.cnf.TG, tgc.WithFloodWait(), tgc.WithRateLimit())

	for _, row := range results {

		if row.Session == "" {
			break
		}
		ids := []int{}

		fileIds := []string{}

		for _, file := range row.Files {
			fileIds = append(fileIds, file.ID)
			for _, part := range file.Parts {
				ids = append(ids, int(part.ID))
			}

		}

		client, _ := tgc.AuthClient(ctx, &c.cnf.TG, row.Session, middlewares...)
		err := tgc.DeleteMessages(ctx, client, row.ChannelId, ids)

		if err != nil {
			c.logger.Error("cron.file_delete_failed", zap.Error(err), zap.Int64("channel_id", row.ChannelId))
			return
		}

		items := pgtype.Array[string]{
			Elements: fileIds,
			Valid:    true,
			Dims:     []pgtype.ArrayDimension{{Length: int32(len(fileIds)), LowerBound: 1}},
		}

		c.db.Where("id = any($1)", items).Delete(&models.File{})

		c.logger.Info("cron.files_cleaned", zap.Int64("user_id", row.UserId), zap.Int64("channel_id", row.ChannelId), zap.Int("file_count", len(fileIds)))
	}
}

func (c *CronService) cleanUploads(ctx context.Context) {
	c.logger.Info("cron.clean_uploads.started")
	var results []uploadResult
	if err := c.db.Table("teldrive.uploads as up").
		Select("JSONB_AGG(up.part_id) as parts,up.channel_id,up.user_id,s.session").
		Joins("LEFT JOIN teldrive.users as u ON u.user_id = up.user_id").
		Joins(`LEFT JOIN (
        SELECT user_id, session
        FROM teldrive.sessions
        WHERE created_at = (
            SELECT MAX(created_at)
            FROM teldrive.sessions s2
            WHERE s2.user_id = sessions.user_id
        )
    ) as s ON u.user_id = s.user_id`).
		Where("up.created_at < ?", time.Now().UTC().Add(-c.cnf.TG.Uploads.Retention)).
		Group("up.channel_id").
		Group("up.user_id").
		Group("s.session").
		Scan(&results).Error; err != nil {
		return
	}

	middlewares := tgc.NewMiddleware(&c.cnf.TG, tgc.WithFloodWait(), tgc.WithRateLimit())
	for _, result := range results {

		if result.Session != "" && len(result.Parts) > 0 {
			client, _ := tgc.AuthClient(ctx, &c.cnf.TG, result.Session, middlewares...)

			err := tgc.DeleteMessages(ctx, client, result.ChannelId, result.Parts)
			if err != nil {
				c.logger.Error("failed to delete messages", zap.Error(err))
				return
			}
		}
		items := pgtype.Array[int]{
			Elements: result.Parts,
			Valid:    true,
			Dims:     []pgtype.ArrayDimension{{Length: int32(len(result.Parts)), LowerBound: 1}},
		}
		c.db.Where("part_id = any(?)", items).Where("channel_id = ?", result.ChannelId).
			Where("user_id = ?", result.UserId).Delete(&models.Upload{}).Delete(&models.Upload{})

	}
}

func (c *CronService) updateFolderSize() {
	c.logger.Info("cron.folder_size.started")
	query := `
	WITH RECURSIVE folder_hierarchy AS (
		SELECT id, id as root_id
		FROM teldrive.files
		WHERE type = 'folder'
		UNION ALL
		SELECT f.id, fh.root_id
		FROM teldrive.files f
		JOIN folder_hierarchy fh ON f.parent_id = fh.id
	),
	folder_sizes AS (
		SELECT root_id, COALESCE(SUM(size), 0) as total_size
		FROM folder_hierarchy fh
		JOIN teldrive.files f ON fh.id = f.id
		WHERE f.type = 'file' AND f.status = 'active'
		GROUP BY root_id
	)
	UPDATE teldrive.files f
	SET size = fs.total_size
	FROM folder_sizes fs
	WHERE f.id = fs.root_id;
	`
	c.db.Exec(query)
}

func (c *CronService) cleanOldEvents() {
	c.db.Exec("DELETE FROM teldrive.events WHERE created_at < NOW() - INTERVAL '5 days';")
}
func (c *CronService) scanTasks(ctx context.Context) {
	// 0. Queue due scheduled tasks (cron/legacy datetime)
	now := time.Now().UTC()
	c.cleanExpiredScanTaskLogs(now)
	c.queueDueScheduledTasks(now)

	// 1. Get Concurrency Limit
	limit := c.cnf.CronJobs.ScanConcurrency
	if limit < 1 {
		limit = 1
	}
	var entry struct {
		Value []byte
	}
	if err := c.db.Table("teldrive.kv").Select("value").Where("key = ?", "scan_concurrency").First(&entry).Error; err == nil {
		if l, err := strconv.Atoi(string(entry.Value)); err == nil && l > 0 {
			limit = l
		}
	}

	// 1. Audit Check
	var totalTasks int64
	c.db.Model(&models.ScanTask{}).Count(&totalTasks)
	c.logger.Info("cron.scan_tasks.audit", zap.Int64("total_db_tasks", totalTasks))

	// 2. Count Active Tasks
	var activeCount int64
	c.db.Model(&models.ScanTask{}).Where("status = ?", "running").Count(&activeCount)

	slotsAvailable := int(int64(limit) - activeCount)
	c.logger.Info("cron.scan_tasks.concurrency", zap.Int("limit", limit), zap.Int64("active", activeCount), zap.Int("slots", slotsAvailable))

	if slotsAvailable <= 0 {
		return
	}

	// 3. Find Waiting Tasks up to available slots
	var tasks []models.ScanTask
	if err := c.db.Where("status = ?", "waiting").Limit(slotsAvailable).Find(&tasks).Error; err != nil {
		c.logger.Error("cron.scan_tasks.query_failed", zap.Error(err))
		return
	}

	if len(tasks) == 0 {
		return
	}

	taskIds := make([]string, len(tasks))
	for i, t := range tasks {
		taskIds[i] = t.ID
	}
	c.logger.Info("cron.scan_tasks.found", zap.Int("count", len(tasks)), zap.Strings("task_ids", taskIds))

	for _, t := range tasks {
		// Atomic update to ensure we claim the task
		res := c.db.Model(&models.ScanTask{}).Where("id = ? AND status = ?", t.ID, "waiting").Update("status", "running")
		if res.RowsAffected == 0 {
			continue // Already claimed by another process
		}

		go func(task models.ScanTask) {
			c.logger.Info("cron.scan_task.starting", zap.String("task_id", task.ID))
			taskCtx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
			defer cancel()

			// Safety defer: if the worker exits and the task is still 'running', mark it as 'failed'
			defer func() {
				var finalTask models.ScanTask
				if err := c.db.First(&finalTask, "id = ?", task.ID).Error; err == nil {
					if finalTask.Status == "running" {
						c.db.Model(&models.ScanTask{}).Where("id = ?", task.ID).Update("status", "failed")
					}
				}
			}()

			worker := services.NewScanTaskWorker(c.db, c.cnf, c.logger)
			if err := worker.ProcessTask(taskCtx, task.ID); err != nil {
				c.logger.Error("cron.scan_task.failed", zap.String("task_id", task.ID), zap.Error(err))
			}
		}(t)
	}
}

func (c *CronService) cleanExpiredScanTaskLogs(now time.Time) {
	retentionDays := c.cnf.CronJobs.ScanLogRetentionDays
	if retentionDays < 1 {
		retentionDays = 3
	}
	threshold := now.AddDate(0, 0, -retentionDays)
	c.db.Model(&models.ScanTask{}).
		Where("updated_at < ?", threshold).
		Where("logs IS NOT NULL").
		Where("logs::text <> '[]'").
		Update("logs", datatypes.JSON([]byte("[]")))
}

func (c *CronService) queueDueScheduledTasks(now time.Time) {
	var scheduledTasks []models.ScanTask
	if err := c.db.Where("schedule <> ''").Where("status IN ?", []string{"scheduled", "completed", "paused", "pause"}).Find(&scheduledTasks).Error; err != nil {
		c.logger.Error("cron.scan_tasks.schedule_query_failed", zap.Error(err))
		return
	}

	for _, task := range scheduledTasks {
		if !c.shouldQueueTask(task, now) {
			continue
		}

		res := c.db.Model(&models.ScanTask{}).
			Where("id = ? AND status = ?", task.ID, task.Status).
			Update("status", "waiting")
		if res.Error != nil {
			c.logger.Error("cron.scan_tasks.schedule_update_failed", zap.String("task_id", task.ID), zap.Error(res.Error))
			continue
		}
		if res.RowsAffected > 0 {
			c.logger.Info("cron.scan_tasks.schedule_queued", zap.String("task_id", task.ID), zap.String("schedule", task.Schedule))
		}
	}
}

func (c *CronService) shouldQueueTask(task models.ScanTask, now time.Time) bool {
	schedule := strings.TrimSpace(task.Schedule)
	if schedule == "" {
		return false
	}

	if spec, err := rcron.ParseStandard(schedule); err == nil {
		// Keep old behavior for standard parser first.
		// Supports 5-field specs and descriptors.
		// fallthrough to extended parser only if needed.
		currentMinute := now.Truncate(time.Minute)
		nextDue := spec.Next(currentMinute.Add(-time.Minute))
		if !nextDue.Equal(currentMinute) || !task.UpdatedAt.Before(currentMinute) {
			return false
		}

		// Non-repeat cron tasks should run once only.
		if !task.RepeatEnabled && task.Status == "completed" {
			return false
		}
		return true
	}

	// Extended cron parser supports optional seconds field.
	extendedParser := rcron.NewParser(
		rcron.SecondOptional | rcron.Minute | rcron.Hour | rcron.Dom | rcron.Month | rcron.Dow | rcron.Descriptor,
	)
	if spec, err := extendedParser.Parse(schedule); err == nil {
		// For cron schedule, queue only when current minute is exactly the next due slot.
		// updated_at guard avoids duplicate re-queues within same minute.
		currentMinute := now.Truncate(time.Minute)
		nextDue := spec.Next(currentMinute.Add(-time.Minute))
		if !nextDue.Equal(currentMinute) || !task.UpdatedAt.Before(currentMinute) {
			return false
		}

		// Non-repeat cron tasks should run once only.
		if !task.RepeatEnabled && task.Status == "completed" {
			return false
		}
		return true
	}

	// Legacy one-time datetime support.
	layouts := []string{
		"2006-01-02T15:04",
		"2006-01-02 15:04",
		"2006-01-02T15:04:05",
		time.RFC3339,
	}
	var when time.Time
	var err error
	for _, layout := range layouts {
		when, err = time.ParseInLocation(layout, schedule, time.Local)
		if err == nil {
			break
		}
	}
	if err != nil {
		c.logger.Debug("cron.scan_tasks.invalid_schedule", zap.String("task_id", task.ID), zap.String("schedule", schedule))
		return false
	}
	if now.Before(when) {
		return false
	}
	// One-time schedule: only queue from pending statuses.
	return task.Status == "scheduled" || task.Status == "paused" || task.Status == "pause"
}