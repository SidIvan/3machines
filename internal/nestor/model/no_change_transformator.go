package model

type NoChangeTransformator[T any] struct {
}

func NewNoChangeTransformator[T any]() *NoChangeTransformator[T] {
	return &NoChangeTransformator[T]{}
}

func (s NoChangeTransformator[T]) Transform(msg T) ([]T, error) {
	return []T{msg}, nil
}
