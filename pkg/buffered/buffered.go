package buffered

type BufferedQueue[T any] struct {
	ch chan T
}

func New[T any](bufferSize uint64) *BufferedQueue[T] {
	// Enforce minimum capacity of 1 to ensure proper bounded buffer semantics.
	// A zero-capacity Go channel is an unbuffered synchronization primitive,
	// not a zero-capacity buffer, which would cause unexpected behavior.
	if bufferSize < 1 {
		bufferSize = 1
	}
	return &BufferedQueue[T]{
		ch: make(chan T, bufferSize),
	}
}

func (q *BufferedQueue[T]) Enqueue(val T) {
	q.ch <- val
}

func (q *BufferedQueue[T]) Dequeue() (val T, ok bool) {
	select {
	case val = <-q.ch:
		return val, true
	default:
		return val, false
	}
}

func (q *BufferedQueue[T]) FreeSlots() uint64 {
	return uint64(cap(q.ch) - len(q.ch))
}

func (q *BufferedQueue[T]) UsedSlots() uint64 {
	return uint64(len(q.ch))
}
