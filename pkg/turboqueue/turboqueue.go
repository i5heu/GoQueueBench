package turboqueue

import (
	"runtime"
	"sync/atomic"
	_ "unsafe" // for go:linkname
)

// CacheLineSize is the typical size of a CPU cache line (64 bytes on most architectures).
// We use 128 bytes (2 cache lines) for extra isolation to avoid prefetcher effects.
const CacheLineSize = 128

// Spin strategy constants - carefully tuned for balanced performance
const (
	// Initial spin iterations with PAUSE (CPU-friendly, low latency)
	pauseSpins = 16
	// Spin iterations before yielding (higher = lower latency, more CPU)
	spinIterations = 64
)

// go:linkname procyield runtime.procyield
// procyield executes PAUSE instructions, which is more CPU-friendly than empty loops
//
//go:linkname procyield runtime.procyield
func procyield(cycles uint32)

// turboCell represents one slot in the ring buffer.
// Each cell is padded to 128 bytes to prevent false sharing between adjacent cells.
// The sequence number is used for lock-free synchronization.
type turboCell[T any] struct {
	sequence uint64    // Sequence number for synchronization
	value    T         // The stored value (8 bytes for pointer)
	_pad     [108]byte // Pad to 128 bytes total (8 + 8 + 108 = 124, aligned to 128)
}

// TurboQueue is an ultra-fast bounded, lock-free, multi-producer/multi-consumer FIFO queue.
//
// Key optimizations:
// 1. Ticket-based reservation for enqueue: Eliminates CAS contention on enqueuePos
// 2. CAS-based dequeue with optimized spin: Best balance of throughput and fairness
// 3. 128-byte cache-line padding: Prevents false sharing and prefetcher interference
// 4. Separated hot/cold data: Position counters on separate cache lines
// 5. Hybrid spin strategy: PAUSE + spin loop + Gosched
type TurboQueue[T any] struct {
	// === Cache Line 1: Enqueue position (hot for producers) ===
	enqueuePos uint64
	_pad1      [CacheLineSize - 8]byte

	// === Cache Line 2: Dequeue position (hot for consumers) ===
	dequeuePos uint64
	_pad2      [CacheLineSize - 8]byte

	// === Cache Line 3: Read-mostly data (cold) ===
	buffer   []turboCell[T]
	mask     uint64 // capacity - 1 (for fast modulo via bitwise AND)
	capacity uint64
	_pad3    [CacheLineSize - 32]byte
}

// New creates a new TurboQueue with the given capacity.
// The capacity is rounded up to the nearest power of 2 for efficient indexing.
func New[T any](capacity uint64) *TurboQueue[T] {
	// Round up to power of 2 for efficient masking
	if capacity < 2 {
		capacity = 2
	}
	if capacity&(capacity-1) != 0 {
		pow := uint64(1)
		for pow < capacity {
			pow <<= 1
		}
		capacity = pow
	}

	q := &TurboQueue[T]{
		buffer:   make([]turboCell[T], capacity),
		mask:     capacity - 1,
		capacity: capacity,
	}

	// Initialize each cell's sequence to its index
	// This indicates that slot i is ready for the i-th enqueue
	for i := uint64(0); i < capacity; i++ {
		q.buffer[i].sequence = i
	}

	return q
}

// spinWait performs a hybrid spin: first CPU PAUSE instructions (very fast),
// then a tight spin loop, then yield to the scheduler.
//
//go:nosplit
func spinWait() {
	// Phase 1: PAUSE instructions - CPU-friendly, ultra-low-latency
	procyield(pauseSpins)

	// Phase 2: Tight spin loop - stays on CPU core
	for i := 0; i < spinIterations; i++ {
		// Intentionally empty
	}

	// Phase 3: Yield to scheduler - fairness for high contention
	runtime.Gosched()
}

// Enqueue inserts a value into the queue.
// This method blocks (busy-waits) if the queue is full.
//
// Uses ticket-based reservation to eliminate CAS contention:
// 1. Atomically reserve a slot by incrementing enqueuePos (no CAS loop)
// 2. Wait until the cell at that position is ready (sequence == pos)
// 3. Write the value and publish by updating sequence to pos+1
func (q *TurboQueue[T]) Enqueue(val T) {
	// Reserve a slot using atomic increment (ticket-based reservation)
	// This eliminates CAS contention - every producer gets a unique slot
	pos := atomic.AddUint64(&q.enqueuePos, 1) - 1
	cell := &q.buffer[pos&q.mask]

	// Wait until the cell is ready for writing
	// The cell is ready when sequence == pos (meaning it's been consumed and recycled)
	for {
		seq := atomic.LoadUint64(&cell.sequence)
		if seq == pos {
			// Cell is ready for writing
			break
		}
		// Cell not ready (queue full) - spin and retry
		spinWait()
	}

	// Write the value (no synchronization needed - we own this slot)
	cell.value = val

	// Publish the value by updating sequence to pos+1
	// This signals to consumers that the cell contains valid data
	atomic.StoreUint64(&cell.sequence, pos+1)
}

// Dequeue removes and returns the oldest value from the queue.
// Returns (value, true) if successful, or (zero, false) if the queue is empty.
//
// Uses CAS because multiple consumers may race for the same slot and
// we want non-blocking behavior when empty.
func (q *TurboQueue[T]) Dequeue() (T, bool) {
	for {
		// Load current dequeue position
		pos := atomic.LoadUint64(&q.dequeuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)

		// Check if the cell contains valid data
		// The cell is ready when sequence == pos+1 (meaning it's been enqueued)
		if seq == pos+1 {
			// Try to claim this slot
			if atomic.CompareAndSwapUint64(&q.dequeuePos, pos, pos+1) {
				// Successfully claimed - read the value
				val := cell.value

				// Mark the cell as free by setting sequence to pos+capacity
				// This makes the cell ready for the (pos+capacity)-th enqueue
				atomic.StoreUint64(&cell.sequence, pos+q.capacity)
				return val, true
			}
			// CAS failed - another consumer got it, retry immediately
		} else {
			// Cell not ready - check if queue is truly empty
			if atomic.LoadUint64(&q.enqueuePos) == pos {
				// Queue is empty
				var zero T
				return zero, false
			}
		}
		// Either CAS failed or cell not ready but queue not empty - spin and retry
		spinWait()
	}
}

// FreeSlots returns an approximate count of available slots in the queue.
// This is approximate because the queue state may change concurrently.
func (q *TurboQueue[T]) FreeSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	used := enq - deq
	if used > q.capacity {
		return 0
	}
	return q.capacity - used
}

// UsedSlots returns an approximate count of items currently in the queue.
// This is approximate because the queue state may change concurrently.
func (q *TurboQueue[T]) UsedSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	used := enq - deq
	if used > q.capacity {
		return q.capacity
	}
	return used
}
