package leases

type ConsistentHashRing struct {
	cfg    Config
	values []uint32
}

func NewConsistentHashRing(cfg Config) *ConsistentHashRing {
	values := make([]uint32, cfg.To-cfg.From)
	for i := range cfg.To - cfg.From {
		values[i] = cfg.From + uint32(i)
	}

	return &ConsistentHashRing{
		cfg:    cfg,
		values: values,
	}
}

func (ring *ConsistentHashRing) Values() []uint32 {
	return ring.values
}
