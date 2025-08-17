package leases

type Status string

func (s Status) String() string {
	return string(s)
}

const (
	Pending Status = "PENDING"
	Leased  Status = "LEASED"
)
