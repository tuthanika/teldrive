package models

import (
	"time"
)

type Channel struct {
	ChannelId   int64     `gorm:"type:bigint;primaryKey"`
	ChannelName string    `gorm:"type:text"`
	TopicId     *int      `gorm:"type:integer"`
	UserId      int64     `gorm:"type:bigint"`
	Selected    bool      `gorm:"type:boolean"`
	CreatedAt   time.Time `gorm:"type:timestamptz"`
}
