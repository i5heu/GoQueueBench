package fastmpmc

import (
	"runtime"
	"sync/atomic"
)

// fastCell represents one slot in the ring buffer.
// We add padding before and after the cell fields to reduce false sharing.
type fastCell[T any] struct {
	_pad0    [8]uint64
	sequence uint64 // Sequence number for synchronization.
	value    T      // Stored value.
	_pad1    [8]uint64
}

// FastMPMCQueue is a bounded, lock‑free multi‑producer/multi‑consumer queue.
type FastMPMCQueue[T any] struct {
	_pad0      [8]uint64
	buffer     []fastCell[T]
	mask       uint64 // mask = capacity - 1 (capacity is a power of 2)
	capacity   uint64
	_pad1      [8]uint64
	enqueuePos uint64 // Next position to enqueue.
	_pad2      [8]uint64
	dequeuePos uint64 // Next position to dequeue.
	_pad3      [8]uint64
}

// New creates a new FastMPMCQueue with the given capacity.
// If the capacity is not a power of 2, it is rounded up.
func New[T any](capacity uint64) *FastMPMCQueue[T] {
	// Round capacity to the next power of 2 if necessary.
	if capacity&(capacity-1) != 0 {
		capPow := uint64(1)
		for capPow < capacity {
			capPow <<= 1
		}
		capacity = capPow
	}

	q := &FastMPMCQueue[T]{
		buffer:   make([]fastCell[T], capacity),
		mask:     capacity - 1,
		capacity: capacity,
	}
	// Initialize the sequence for each cell.
	for i := uint64(0); i < capacity; i++ {
		q.buffer[i].sequence = i
	}
	return q
}

// Enqueue inserts a value into the queue.
// It busy-waits until a slot becomes available.
func (q *FastMPMCQueue[T]) Enqueue(val T) {
	for {
		// Load current enqueue position.
		pos := atomic.LoadUint64(&q.enqueuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		// The cell is ready for writing if seq equals pos.
		if int64(seq)-int64(pos) == 0 {
			// Try to claim the slot by incrementing enqueuePos.
			if atomic.CompareAndSwapUint64(&q.enqueuePos, pos, pos+1) {
				cell.value = val
				// Publish the value: set sequence to pos+1.
				atomic.StoreUint64(&cell.sequence, pos+1)
				return
			}
		} else if int64(seq)-int64(pos) < 0 {
			// The slot is not yet free (queue is full).
			runtime.Gosched()
		} else {
			// Some other producer has already updated the slot.
			runtime.Gosched()
		}
	}
}

// Dequeue removes and returns a value from the queue.
// If the queue is empty, it returns the zero value and false.
func (q *FastMPMCQueue[T]) Dequeue() (T, bool) {
	// Attempt a single try; the caller can retry if necessary.
	pos := atomic.LoadUint64(&q.dequeuePos)
	cell := &q.buffer[pos&q.mask]
	seq := atomic.LoadUint64(&cell.sequence)
	// Check if the cell contains a value: sequence should equal pos+1.
	if int64(seq)-int64(pos+1) == 0 {
		// Claim the slot.
		if atomic.CompareAndSwapUint64(&q.dequeuePos, pos, pos+1) {
			val := cell.value
			// Mark the cell as free by setting its sequence to pos+capacity.
			atomic.StoreUint64(&cell.sequence, pos+q.capacity)
			return val, true
		}
	} else {
		runtime.Gosched()
	}
	var zero T
	return zero, false
}

// FreeSlots returns an approximate count of free slots in the queue.
func (q *FastMPMCQueue[T]) FreeSlots() uint64 {
	return q.capacity - q.UsedSlots()
}

// UsedSlots returns an approximate count of occupied slots.
func (q *FastMPMCQueue[T]) UsedSlots() uint64 {
	// The difference between enqueuePos and dequeuePos gives an approximation.
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	return enq - deq
}
