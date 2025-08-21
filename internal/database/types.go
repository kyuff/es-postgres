package database

type OutboxWatermark struct {
	Watermark  int64
	RetryCount int64
	StreamID   string
}
