package iter

type Seq func(yield func() bool)
type Seq1[T any] func(yield func(T) bool)
type Seq2[T1, T2 any] func(yield func(T1, T2) bool)
