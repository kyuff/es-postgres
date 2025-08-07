package database

type Stream struct {
	ID   string
	Type string
}

type OutboxWatermark struct {
	Watermark  int64
	RetryCount int64
}
