package models

import (
	"time"

	"gorm.io/datatypes"
)

type ScanTask struct {
	ID            string         `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	UserID        int64          `gorm:"type:bigint;not null" json:"userId"`
	FolderID      string         `gorm:"type:varchar(255)" json:"folderId"`
	ChannelID     int64          `gorm:"type:bigint;not null" json:"channelId"`
	TopicID       *int64         `gorm:"type:bigint" json:"topicId"`
	SplitMode     bool           `gorm:"type:boolean;default:false" json:"splitMode"`
	RuleMode      bool           `gorm:"type:boolean;default:false" json:"ruleMode"`
	RuleString    string         `gorm:"type:text" json:"ruleString"`
	RepeatEnabled bool           `gorm:"type:boolean;default:false" json:"repeatEnabled"`
	Schedule      string         `gorm:"type:text" json:"schedule"`
	Status        string         `gorm:"type:varchar(50);default:'waiting'" json:"status"`
	ScannedCount  int            `gorm:"type:int;default:0" json:"scannedCount"`
	ImportedCount int            `gorm:"type:int;default:0" json:"importedCount"`
	Logs          datatypes.JSON `gorm:"type:jsonb" json:"logs"`
	FolderName    string         `gorm:"-" json:"folderName"`
	FolderPath    string         `gorm:"-" json:"folderPath"`
	ChannelName   string         `json:"channelName"`
	TopicName     string         `json:"topicName"`
	CreatedAt     time.Time      `gorm:"default:timezone('utc'::text, now())" json:"createdAt"`
	UpdatedAt     time.Time      `gorm:"default:timezone('utc'::text, now())" json:"updatedAt"`
}
