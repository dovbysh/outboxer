package event

import "time"

type Outbox struct {
	tableName     struct{} `pg:"_natss_outbox"`
	ID            uint64   `pg:",pk"`
	Published     bool     `pg:",use_zero,notnull,default:false"`
	PublishedNUID string   `pg:"published_nuid,type:varchar(22)"`
	CreatedAt     time.Time
	PublishedAt   time.Time
	Subject       string
	Data          []byte
}
