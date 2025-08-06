package postgres

type valuer interface {
	Values() []uint32
}

func newFixedSizeValuer(size int) valuer {
	values := make([]uint32, size)
	for i := 0; i < size; i++ {
		values[i] = uint32(i)
	}

	return &fixedSizeValuer{
		values: values,
	}
}

type fixedSizeValuer struct {
	values []uint32
}

func (v *fixedSizeValuer) Values() []uint32 {
	return v.values
}
