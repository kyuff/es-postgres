package database

type Stream struct {
	StoreID string
	Type    string
}

type OutboxWatermark struct {
	Watermark  int64
	RetryCount int64
	StreamID   string
}
