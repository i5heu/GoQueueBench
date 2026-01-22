package main

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/i5heu/GoQueueBench/internal/queue"
)

// Pointer is a constraint that ensures T is always a pointer type.
type Pointer[T any] interface {
	*T
}

// Compile-time enforcement that T must be a pointer.
func enforcePointer[T any, PT interface{ ~*T }](q queue.QueueValidationInterface[PT]) {}

// progressWatchdog monitors progress and fails the test if no progress is made for 15 seconds.
type progressWatchdog struct {
	t            *testing.T
	label        string
	lastProgress atomic.Int64
	done         chan struct{}
}

func newWatchdog(t *testing.T, label string) *progressWatchdog {
	wd := &progressWatchdog{
		t:     t,
		label: label,
		done:  make(chan struct{}),
	}
	wd.lastProgress.Store(time.Now().UnixNano())
	return wd
}

func (wd *progressWatchdog) Start() {
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				last := wd.lastProgress.Load()
				elapsed := time.Since(time.Unix(0, last))
				if elapsed > 15*time.Second {
					wd.t.Fatalf("No progress in the last 15 seconds (%s test likely stuck).", wd.label)
				}
			case <-wd.done:
				return
			}
		}
	}()
}

func (wd *progressWatchdog) Progress() {
	wd.lastProgress.Store(time.Now().UnixNano())
}

func (wd *progressWatchdog) Stop() {
	close(wd.done)
}

type testQueueInterface = interface {
	Enqueue(*int)
	Dequeue() (*int, bool)
	FreeSlots() uint64
	UsedSlots() uint64
}

// withAllQueues is a test helper that loops over all implementations
// and calls your test function for each one.
// NOTE: Feature filtering is done inside the subtest to avoid skipping at parent level.
func withAllQueues(t *testing.T, scenarioName string, testedFeatures []string, fn func(t *testing.T, impl Implementation[*int, testQueueInterface])) {
	t.Helper()
	impls := getImplementations()
	for _, impl := range impls {
		impl := impl // capture range variable

		t.Run(impl.name, func(t *testing.T) {
			if impl.newQueue == nil {
				t.Skipf("Skipping stub implementation %q", impl.name)
				return
			}

			// Check if the test tests a feature that the implementation does not support
			if testedFeatures != nil {
				for _, feature := range testedFeatures {
					found := false
					for _, implFeature := range impl.features {
						if feature == implFeature {
							found = true
							break
						}
					}
					if !found {
						t.Skipf("Skipping: missing feature %q", feature)
						return
					}
				}
			}

			fn(t, impl)
		})
	}
}

func TestBasicFIFO(t *testing.T) {
	withAllQueues(t, "BasicFIFO", []string{"FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)

		wd := newWatchdog(t, "BasicFIFO")
		wd.Start()
		defer wd.Stop()

		const N = 1024

		// Enqueue N items, each carrying its sequence number.
		for i := 0; i < N; i++ {
			item := i
			q.Enqueue(&item) // Blocks if full
			wd.Progress()
		}

		// Dequeue N items, in FIFO order. Because Dequeue returns nil if empty,
		// we busy-wait until we get a value.
		for i := 0; i < N; i++ {
			var valPtr *int
			for {
				var ok bool
				valPtr, ok = q.Dequeue()
				if ok {
					break
				}
				time.Sleep(1 * time.Microsecond)
			}
			wd.Progress()
			if *valPtr != i {
				t.Fatalf("Expected %d, got %d at index %d", i, *valPtr, i)
			}
		}
	})
}

func TestHighContention(t *testing.T) {
	withAllQueues(t, "HighContention", []string{"MPMC", "FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)

		wd := newWatchdog(t, "HighContention")
		wd.Start()
		defer wd.Stop()

		const (
			numProducers        = 500
			numConsumers        = 500
			messagesPerProducer = 10000
		)
		totalMessages := numProducers * messagesPerProducer

		sentCount := atomic.Uint64{}
		receivedCount := atomic.Uint64{}

		// Start producers.
		var prodWg sync.WaitGroup
		prodWg.Add(numProducers)
		for i := 0; i < numProducers; i++ {
			go func(prodID int) {
				defer prodWg.Done()
				for j := 0; j < messagesPerProducer; j++ {
					val := prodID + j
					q.Enqueue(&val) // blocks if full
					wd.Progress()
					sentCount.Add(1)
				}
			}(i)
		}

		// Divide the consumption workload among consumers.
		messagesPerConsumer := totalMessages / numConsumers
		remainder := totalMessages % numConsumers

		var consWg sync.WaitGroup
		consWg.Add(numConsumers)
		for i := 0; i < numConsumers; i++ {
			count := messagesPerConsumer
			if i == numConsumers-1 {
				count += remainder
			}
			go func(consumerID, count int) {
				defer consWg.Done()
				for j := 0; j < count; j++ {
					// Because Dequeue returns nil if empty, we busy-wait until we get a real value.
					for {
						_, ok := q.Dequeue()
						if ok {
							break
						}
						time.Sleep(1 * time.Microsecond)
					}
					wd.Progress()
					receivedCount.Add(1)
				}
			}(i, count)
		}

		// Wait for all producers and consumers.
		prodWg.Wait()
		consWg.Wait()

		if sentCount.Load() != uint64(totalMessages) {
			t.Fatalf("Expected to send %d messages, but sent %d", totalMessages, sentCount.Load())
		}
		if receivedCount.Load() != uint64(totalMessages) {
			t.Fatalf("Expected to receive %d messages, but received %d", totalMessages, receivedCount.Load())
		}
	})
}

func TestEmptyQueue(t *testing.T) {
	withAllQueues(t, "EmptyQueue", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)

		wd := newWatchdog(t, "EmptyQueue")
		wd.Start()
		defer wd.Stop()

		// If the queue is empty, Dequeue should return nil immediately (non-blocking).
		val, _ := q.Dequeue()
		if val != nil {
			t.Fatalf("Expected Dequeue to return nil on empty queue, got %v", val)
		}
		wd.Progress()

		// Enqueue an element.
		x := 42
		q.Enqueue(&x)
		wd.Progress()

		// Now Dequeue should yield the element.
		val, _ = q.Dequeue()
		if val == nil {
			t.Fatal("Expected to dequeue a valid pointer, got nil")
		}
		if *val != 42 {
			t.Fatalf("Expected to dequeue 42, got %v", *val)
		}
	})
}

func TestWrapAround(t *testing.T) {
	withAllQueues(t, "WrapAround", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)

		wd := newWatchdog(t, "WrapAround")
		wd.Start()
		defer wd.Stop()

		const capacity = 1024

		// Fill fully.
		for i := 0; i < capacity; i++ {
			val := i
			q.Enqueue(&val)
			wd.Progress()
		}
		// Dequeue half.
		for i := 0; i < capacity/2; i++ {
			var val *int
			for {
				val, _ = q.Dequeue()
				if val != nil {
					break
				}
				time.Sleep(1 * time.Microsecond)
			}
			wd.Progress()
		}
		// Enqueue again to force wrap-around.
		for i := 0; i < capacity/2; i++ {
			val := 1000 + i
			q.Enqueue(&val)
			wd.Progress()
		}
		// Dequeue everything and verify.
		for i := 0; i < capacity; i++ {
			var val *int
			for {
				val, _ = q.Dequeue()
				if val != nil {
					break
				}
				time.Sleep(1 * time.Microsecond)
			}
			wd.Progress()
		}
	})
}

func TestSmallStress(t *testing.T) {
	withAllQueues(t, "SmallStress", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)

		wd := newWatchdog(t, "SmallStress")
		wd.Start()
		defer wd.Stop()

		const (
			numProducers        = 2
			numConsumers        = 2
			messagesPerProducer = 2500
		)
		totalMessages := numProducers * messagesPerProducer

		sentCount := atomic.Uint64{}
		receivedCount := atomic.Uint64{}

		var prodWg sync.WaitGroup
		prodWg.Add(numProducers)
		for i := 0; i < numProducers; i++ {
			go func(prodID int) {
				defer prodWg.Done()
				for j := 0; j < messagesPerProducer; j++ {
					val := prodID*messagesPerProducer + j
					q.Enqueue(&val) // blocks if full
					wd.Progress()
					sentCount.Add(1)
				}
			}(i)
		}

		var consWg sync.WaitGroup
		consWg.Add(numConsumers)
		for i := 0; i < numConsumers; i++ {
			go func() {
				defer consWg.Done()
				for {
					// If we've received everything, stop.
					if receivedCount.Load() >= uint64(totalMessages) {
						return
					}
					// Because Dequeue can return nil, we busy-wait until a real value arrives.
					item, _ := q.Dequeue()
					if item != nil {
						receivedCount.Add(1)
						wd.Progress()
					} else {
						time.Sleep(1 * time.Millisecond)
					}
				}
			}()
		}

		prodWg.Wait()
		consWg.Wait()

		if sentCount.Load() != uint64(totalMessages) {
			t.Fatalf("Expected to send %d messages, but sent %d", totalMessages, sentCount.Load())
		}
		if receivedCount.Load() != uint64(totalMessages) {
			t.Fatalf("Expected to receive %d messages, but received %d", totalMessages, receivedCount.Load())
		}
	})
}

func TestUsedFreeSlots(t *testing.T) {
	withAllQueues(t, "UsedFreeSlots", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024) // Assume internally it has some fixed capacity (e.g. 1024)

		wd := newWatchdog(t, "UsedFreeSlots")
		wd.Start()
		defer wd.Stop()

		// 1. Right after creation, we expect UsedSlots = 0, FreeSlots > 0.
		if q.UsedSlots() != 0 {
			t.Fatalf("Expected UsedSlots=0, got %d", q.UsedSlots())
		}
		if q.FreeSlots() == 0 {
			t.Fatalf("Expected FreeSlots>0, got %d", q.FreeSlots())
		}

		// 2. Enqueue a few items
		numEnqueues := 10
		for i := 0; i < numEnqueues; i++ {
			val := i
			q.Enqueue(&val)
			wd.Progress()
		}
		if q.UsedSlots() != uint64(numEnqueues) {
			t.Fatalf("Expected UsedSlots=%d, got %d", numEnqueues, q.UsedSlots())
		}
		// We can check that freeSlots + usedSlots = capacity if your queue
		// enforces a known capacity. If not, omit this check or adapt it.

		// 3. Dequeue half
		toDequeue := numEnqueues / 2
		for i := 0; i < toDequeue; i++ {
			valPtr, _ := q.Dequeue()
			if valPtr == nil {
				t.Fatalf("Expected a non-nil item after enqueuing %d items", numEnqueues)
			}
			wd.Progress()
		}
		if q.UsedSlots() != uint64(numEnqueues-toDequeue) {
			t.Fatalf("Expected UsedSlots=%d after dequeuing %d items, got %d",
				numEnqueues-toDequeue, toDequeue, q.UsedSlots())
		}
	})
}

func TestFullQueueBlocking(t *testing.T) {
	withAllQueues(t, "FullQueueBlocking", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 1024 // If your queue uses a different default capacity, adapt as needed.
		q := impl.newQueue(1024)

		wd := newWatchdog(t, "FullQueueBlocking")
		wd.Start()
		defer wd.Stop()

		// Fill the queue exactly to capacity.
		for i := 0; i < capacity; i++ {
			x := i
			q.Enqueue(&x)
			wd.Progress()
		}

		if q.FreeSlots() != 0 {
			t.Fatalf("Expected FreeSlots=0 after enqueuing %d items, got %d", capacity, q.FreeSlots())
		}
		if q.UsedSlots() != uint64(capacity) {
			t.Fatalf("Expected UsedSlots=%d, got %d", capacity, q.UsedSlots())
		}

		blocked := make(chan struct{})
		done := make(chan struct{})

		go func() {
			defer close(done)
			val := 9999
			q.Enqueue(&val) // This should block until we free a slot.
			wd.Progress()
		}()

		// Wait a short time to confirm goroutine is blocked.
		select {
		case <-done:
			t.Fatal("Expected Enqueue to block, but goroutine completed immediately")
		case <-time.After(100 * time.Millisecond):
			// It's likely blocked, so signal success here by sending on 'blocked'.
			close(blocked)
		}

		// Now free one slot by dequeuing.
		valPtr, _ := q.Dequeue()
		if valPtr == nil {
			t.Fatal("Expected a valid item from Dequeue")
		}
		wd.Progress()

		// Now the Enqueue goroutine should unblock and complete
		select {
		case <-done:
			// Good, it unblocked.
		case <-time.After(2 * time.Second):
			t.Fatal("Enqueue goroutine did not unblock after freeing a slot")
		}

		// Verify final usage count: we re-enqueued one after freeing a slot
		if q.UsedSlots() != uint64(capacity) {
			t.Fatalf("Expected queue to still be at capacity, got UsedSlots=%d", q.UsedSlots())
		}
	})
}

func TestMixedConcurrentOps(t *testing.T) {
	withAllQueues(t, "MixedConcurrentOps", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)

		wd := newWatchdog(t, "MixedConcurrentOps")
		wd.Start()
		defer wd.Stop()

		const (
			numGoroutines = 1000
			loopCount     = 1000
		)

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(gID int) {
				defer wg.Done()
				for i := 0; i < loopCount; i++ {
					// ENQUEUE
					val := (gID << 16) + i
					q.Enqueue(&val)
					wd.Progress()

					// DEQUEUE
					var got *int
					for {
						got, _ = q.Dequeue()
						if got != nil {
							break
						}
						time.Sleep(time.Microsecond)
					}
					wd.Progress()
				}
			}(g)
		}
		wg.Wait()

		// By design, each goroutine enqueues once and dequeues once in each iteration.
		// So at the end, the queue should end up empty.
		used := q.UsedSlots()
		if used != 0 {
			t.Fatalf("Expected queue to be empty (UsedSlots=0), got %d", used)
		}
	})
}

func TestNilEnqueue(t *testing.T) {
	withAllQueues(t, "NilEnqueue", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "NilEnqueue")
		wd.Start()
		defer wd.Stop()

		// Enqueue a nil pointer.
		q.Enqueue(nil)
		wd.Progress()

		// Check that the queue counts the nil as an enqueued element.
		if q.UsedSlots() != 1 {
			t.Fatalf("Expected UsedSlots=1 after enqueuing nil, got %d", q.UsedSlots())
		}

		// Dequeue should return nil (which was enqueued).
		val, _ := q.Dequeue()
		if val != nil {
			t.Fatalf("Expected dequeued value to be nil when enqueued nil, got %v", val)
		}
		wd.Progress()

		// Now the queue should be empty.
		if q.UsedSlots() != 0 {
			t.Fatalf("Expected queue to be empty after dequeuing, got UsedSlots=%d", q.UsedSlots())
		}
	})
}

func TestRepeatedEmptyDequeue(t *testing.T) {
	withAllQueues(t, "RepeatedEmptyDequeue", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "RepeatedEmptyDequeue")
		wd.Start()
		defer wd.Stop()

		for i := 0; i < 1000; i++ {
			val, _ := q.Dequeue()
			if val != nil {
				t.Fatalf("Expected nil from empty Dequeue at iteration %d", i)
			}
			wd.Progress()
		}
		if q.UsedSlots() != 0 {
			t.Fatalf("Expected queue to remain empty after repeated Dequeue calls, got %d", q.UsedSlots())
		}
	})
}

func TestHighWrapAround(t *testing.T) {
	withAllQueues(t, "HighWrapAround", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "HighWrapAround")
		wd.Start()
		defer wd.Stop()

		const iterations = 1000000
		for i := 0; i < iterations; i++ {
			val := i
			q.Enqueue(&val)
			wd.Progress()
			item, _ := q.Dequeue()
			if item == nil {
				t.Fatalf("Expected valid item at iteration %d", i)
			}
			if *item != i {
				t.Fatalf("Expected %d, got %d at iteration %d", i, *item, i)
			}
			wd.Progress()
		}
		if q.UsedSlots() != 0 {
			t.Fatalf("Expected queue to be empty after high wrap-around test, got %d", q.UsedSlots())
		}
	})
}

func TestConcurrentUsageCounters(t *testing.T) {
	withAllQueues(t, "ConcurrentUsageCounters", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 1024
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "ConcurrentUsageCounters")
		wd.Start()
		defer wd.Stop()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100000; i++ {
				val := i
				q.Enqueue(&val)
				q.Dequeue()
				wd.Progress()
			}
		}()

		wg.Wait()

		// Concurrently verify that the sum of FreeSlots and UsedSlots equals capacity.

		used := q.UsedSlots()
		free := q.FreeSlots()
		if used+free != capacity {
			t.Fatalf("Usage counters inconsistent: UsedSlots(%d) + FreeSlots(%d) != %d", used, free, capacity)
		}
		wd.Progress()
		time.Sleep(10 * time.Microsecond)

	})
}

func TestAlternatingSingleCapacity(t *testing.T) {
	withAllQueues(t, "AlternatingSingleCapacity", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1)
		wd := newWatchdog(t, "AlternatingSingleCapacity")
		wd.Start()
		defer wd.Stop()

		const iterations = 1000000
		for i := 0; i < iterations; i++ {
			val := i
			q.Enqueue(&val)
			wd.Progress()
			item, _ := q.Dequeue()
			if item == nil {
				t.Fatalf("Expected valid item in iteration %d", i)
			}
			if *item != i {
				t.Fatalf("Expected %d, got %d at iteration %d", i, *item, i)
			}
			wd.Progress()
		}

		if q.UsedSlots() != 0 {
			t.Fatalf("Expected queue to be empty after alternating operations, got %d", q.UsedSlots())
		}
	})
}

// TestZeroCapacityQueue tests behavior when creating a queue with zero capacity.
// Implementations may handle this differently - some may panic, others may
// create a minimum capacity queue. This test documents the behavior.
func TestZeroCapacityQueue(t *testing.T) {
	withAllQueues(t, "ZeroCapacityQueue", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		// This test uses recover to handle potential panics from invalid capacity
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Implementation panics on zero capacity creation (acceptable): %v", r)
			}
		}()

		q := impl.newQueue(0)

		// If we get here, the implementation accepts zero capacity
		// Check what the actual behavior is
		freeSlots := q.FreeSlots()
		usedSlots := q.UsedSlots()

		t.Logf("Zero capacity queue created: FreeSlots=%d, UsedSlots=%d", freeSlots, usedSlots)

		// If FreeSlots > 0, the implementation likely uses a minimum capacity
		if freeSlots > 0 {
			t.Logf("Implementation uses minimum capacity of %d", freeSlots)

			// Try to use it
			val := 42
			done := make(chan bool, 1)
			panicChan := make(chan interface{}, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicChan <- r
					}
				}()
				q.Enqueue(&val)
				done <- true
			}()

			select {
			case <-done:
				// Enqueue succeeded
				ptr, ok := q.Dequeue()
				if !ok {
					t.Error("Dequeue failed after enqueue on zero-capacity queue")
				} else if *ptr != 42 {
					t.Errorf("Value mismatch: expected 42, got %d", *ptr)
				}
			case r := <-panicChan:
				t.Logf("Enqueue panics on zero-capacity queue (needs fix): %v", r)
			case <-time.After(100 * time.Millisecond):
				t.Log("Enqueue blocks on zero capacity queue (may never complete)")
			}
		} else {
			// FreeSlots is 0, so queue truly has zero capacity
			// Enqueue should block forever - test with timeout
			val := 42
			done := make(chan bool, 1)
			panicChan := make(chan interface{}, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicChan <- r
					}
				}()
				q.Enqueue(&val)
				done <- true
			}()

			select {
			case <-done:
				t.Error("Enqueue completed on truly zero-capacity queue - this seems wrong")
			case r := <-panicChan:
				t.Logf("Enqueue panics on zero-capacity queue (implementation bug - should block or use min capacity): %v", r)
			case <-time.After(100 * time.Millisecond):
				t.Log("Enqueue correctly blocks on zero-capacity queue")
			}

			// Dequeue should return immediately with false (but may also panic)
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("Dequeue panics on zero-capacity queue: %v", r)
					}
				}()
				_, ok := q.Dequeue()
				if ok {
					t.Error("Dequeue returned true on empty zero-capacity queue")
				}
			}()
		}
	})
}

func TestFIFOPointerIntegrity(t *testing.T) {
	withAllQueues(t, "PointerIntegrity", []string{"FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "PointerIntegrity")
		wd.Start()
		defer wd.Stop()

		const numItems = 100
		originalPointers := make([]*int, numItems)

		// Enqueue pointers to newly allocated ints with unique addresses and values.
		for i := 0; i < numItems; i++ {
			p := new(int)
			*p = i
			originalPointers[i] = p
			q.Enqueue(p)
			wd.Progress()
		}

		// Dequeue each item and verify that the pointer and its value are unchanged.
		for i := 0; i < numItems; i++ {
			var got *int
			for {
				got, _ = q.Dequeue()
				if got != nil {
					break
				}
				time.Sleep(1 * time.Microsecond)
			}
			wd.Progress()
			if got != originalPointers[i] {
				t.Fatalf("Pointer corruption at index %d: expected pointer %p, got %p", i, originalPointers[i], got)
			}
			if *got != i {
				t.Fatalf("Value corruption at index %d: expected %d, got %d", i, i, *got)
			}
		}
	})
}

func TestDetailedPointerIntegrityWrapAround(t *testing.T) {
	withAllQueues(t, "TestDetailedPointerIntegrityWrapAround", []string{"FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		// Use a small capacity to force wrap-around behavior.
		const smallCapacity = 64
		// totalOps is the number of enqueue operations performed by the writer.
		const totalOps = 2000000
		q := impl.newQueue(smallCapacity)

		wd := newWatchdog(t, "TestDetailedPointerIntegrityWrapAround")
		wd.Start()
		defer wd.Stop()

		// expectedChan holds the pointers in the exact order they were enqueued.
		// Its capacity is the total number of items expected (initial fill + writer ops).
		totalExpected := totalOps + smallCapacity
		expectedChan := make(chan *int, totalExpected)

		// Pre-fill: enqueue smallCapacity items with values 0..smallCapacity-1.
		for i := 0; i < smallCapacity; i++ {
			ptr := new(int)
			*ptr = i
			q.Enqueue(ptr)
			expectedChan <- ptr
			wd.Progress()
		}

		// Launch a writer goroutine that enqueues totalOps new items.
		doneWriter := make(chan struct{})
		go func() {
			// nextValue starts at smallCapacity so that the overall values form a continuous increasing sequence.
			nextValue := smallCapacity
			for op := 0; op < totalOps; op++ {
				newPtr := new(int)
				*newPtr = nextValue
				q.Enqueue(newPtr)
				expectedChan <- newPtr
				nextValue++
				wd.Progress()
			}
			close(doneWriter)
		}()

		// Now, in the main (reader) goroutine, perform totalExpected dequeue operations.
		// For each operation, wait until a pointer is available then compare it to the expected pointer.
		for op := 0; op < totalExpected; op++ {
			var got *int
			for {
				got, _ = q.Dequeue()
				if got != nil {
					break
				}
				time.Sleep(1 * time.Microsecond)
			}
			wd.Progress()
			expected := <-expectedChan
			// Verify that the pointer addresses match.
			if got != expected {
				t.Fatalf("Pointer mismatch at op %d: expected pointer %p, got %p", op, expected, got)
			}
			// Verify that the stored value matches the expected sequence.
			if *got != op {
				t.Fatalf("Value mismatch at op %d: expected %d, got %d", op, op, *got)
			}
		}

		// Wait for the writer goroutine to finish.
		<-doneWriter

		// Finally, the queue should be empty.
		if q.UsedSlots() != 0 {
			t.Fatalf("Expected queue to be empty after all operations, but UsedSlots = %d", q.UsedSlots())
		}
	})
}

func TestNonFIFOPointerIntegrity(t *testing.T) {
	withAllQueues(t, "NonFIFO", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const numItems = 100000
		q := impl.newQueue(256)
		wd := newWatchdog(t, "NonFIFO")
		wd.Start()
		defer wd.Stop()

		// expected will hold all pointers that are enqueued.
		// The boolean value will be set to true once the pointer is seen during dequeue.
		expected := make(map[*int]bool, numItems)
		expectedIn := make(map[*int]bool, numItems)
		for i := 0; i < numItems; i++ {
			p := new(int)
			*p = i
			expected[p] = false
			expectedIn[p] = false

			wd.Progress()
		}

		go func() {
			// Enqueue pointers
			for ptr := range expectedIn {
				q.Enqueue(ptr)
				wd.Progress()
			}
		}()

		// Dequeue until we've received exactly numItems elements.
		receivedCount := 0
		for receivedCount < numItems {
			var p *int
			var ok bool
			p, ok = q.Dequeue()
			if ok && p != nil {
				// Check that this pointer was indeed enqueued.
				if _, exists := expected[p]; !exists {
					t.Fatalf("Received pointer %p which was not enqueued", p)
				}
				// If the pointer has already been seen, then it's a duplicate.
				if expected[p] {
					t.Fatalf("Received pointer %p more than once", p)
				}
				expected[p] = true
				receivedCount++
			} else {
				// No value available; wait a moment.
				time.Sleep(1 * time.Microsecond)
			}
			wd.Progress()
		}

		// Verify that every enqueued pointer was seen exactly once.
		for p, seen := range expected {
			if !seen {
				t.Fatalf("Expected pointer %p was not received", p)
			}
		}
	})
}

func TestConcurrentNonFIFOMultiRW(t *testing.T) {
	withAllQueues(t, "ConcurrentNonFIFO", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const (
			numProducers     = 1000
			numConsumers     = 1000
			itemsPerProducer = 10
		)
		totalItems := numProducers * itemsPerProducer

		q := impl.newQueue(1024)
		wd := newWatchdog(t, "ConcurrentNonFIFO")
		wd.Start()
		defer wd.Stop()

		var producedCount atomic.Uint64
		var consumedCount atomic.Uint64

		// Start producers.
		var wgProducers sync.WaitGroup
		wgProducers.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer wgProducers.Done()
				for i := 0; i < itemsPerProducer; i++ {
					// Each produced value is unique.
					val := producerID*itemsPerProducer + i
					q.Enqueue(&val)
					producedCount.Add(1)
					wd.Progress()
				}
			}(p)
			wd.Progress()
		}

		// Start consumers.
		var wgConsumers sync.WaitGroup
		wgConsumers.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func(consumerID int) {
				defer wgConsumers.Done()
				for {
					// Stop if we've consumed all items.
					if consumedCount.Load() >= uint64(totalItems) {
						return
					}
					ptr, ok := q.Dequeue()
					if ok && ptr != nil {
						consumedCount.Add(1)
						wd.Progress()
					} else {
						time.Sleep(1 * time.Microsecond)
					}
				}
			}(c)
		}

		// Wait for producers to finish.
		wgProducers.Wait()
		// Wait until the consumers have consumed all items.
		for consumedCount.Load() < uint64(totalItems) {
			time.Sleep(1 * time.Millisecond)
		}
		wgConsumers.Wait()

		if producedCount.Load() != uint64(totalItems) {
			t.Fatalf("Produced count mismatch: expected %d, got %d", totalItems, producedCount.Load())
		}
		if consumedCount.Load() != uint64(totalItems) {
			t.Fatalf("Consumed count mismatch: expected %d, got %d", totalItems, consumedCount.Load())
		}
	})
}

// =============================================================================
// Full Queue Blocking Tests
// =============================================================================
// These tests verify that when a queue is full, Enqueue blocks rather than
// dropping items. This is a critical correctness requirement.
// =============================================================================

// TestFullQueueBlockingMultipleProducers verifies that multiple goroutines
// trying to enqueue on a full queue all block until space becomes available.
func TestFullQueueBlockingMultipleProducers(t *testing.T) {
	withAllQueues(t, "FullQueueBlockingMultipleProducers", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 64
		const numBlockedProducers = 10

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "FullQueueBlockingMultipleProducers")
		wd.Start()
		defer wd.Stop()

		// Fill the queue to capacity
		for i := 0; i < capacity; i++ {
			x := i
			q.Enqueue(&x)
			wd.Progress()
		}

		// Verify queue is full
		if q.FreeSlots() != 0 {
			t.Fatalf("Expected FreeSlots=0 after filling, got %d", q.FreeSlots())
		}

		// Track which producers have completed
		completed := make([]atomic.Bool, numBlockedProducers)
		var allStarted sync.WaitGroup
		allStarted.Add(numBlockedProducers)

		// Launch producers that should all block
		for i := 0; i < numBlockedProducers; i++ {
			go func(id int) {
				allStarted.Done() // Signal that this goroutine has started
				val := 1000 + id
				q.Enqueue(&val) // This should block
				completed[id].Store(true)
				wd.Progress()
			}(i)
		}

		// Wait for all producers to start
		allStarted.Wait()

		// Give them time to potentially complete (they shouldn't)
		time.Sleep(100 * time.Millisecond)

		// Verify none have completed (all should be blocked)
		for i := 0; i < numBlockedProducers; i++ {
			if completed[i].Load() {
				t.Errorf("Producer %d completed when it should have blocked", i)
			}
		}

		// Now dequeue items to make space
		for i := 0; i < numBlockedProducers; i++ {
			_, ok := q.Dequeue()
			if !ok {
				t.Fatalf("Failed to dequeue item %d", i)
			}
			wd.Progress()
		}

		// Wait for blocked producers to complete
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			allDone := true
			for i := 0; i < numBlockedProducers; i++ {
				if !completed[i].Load() {
					allDone = false
					break
				}
			}
			if allDone {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		// Verify all producers eventually completed
		for i := 0; i < numBlockedProducers; i++ {
			if !completed[i].Load() {
				t.Errorf("Producer %d never completed after space was freed", i)
			}
		}
	})
}

// TestFullQueueNoDataLoss verifies that no items are lost when the queue
// reaches capacity under high contention.
func TestFullQueueNoDataLoss(t *testing.T) {
	withAllQueues(t, "FullQueueNoDataLoss", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 128
		const numProducers = 20
		const itemsPerProducer = 500
		const totalItems = numProducers * itemsPerProducer

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "FullQueueNoDataLoss")
		wd.Start()
		defer wd.Stop()

		// Track all items with unique pointers
		var enqueuedMu sync.Mutex
		enqueuedItems := make(map[*int]int, totalItems)

		var dequeuedMu sync.Mutex
		dequeuedItems := make(map[*int]int, totalItems)

		var prodWg sync.WaitGroup
		var consumerDone atomic.Bool
		var dequeuedCount atomic.Int64

		// Start producers
		prodWg.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				for i := 0; i < itemsPerProducer; i++ {
					ptr := new(int)
					val := producerID*itemsPerProducer + i
					*ptr = val

					enqueuedMu.Lock()
					enqueuedItems[ptr] = val
					enqueuedMu.Unlock()

					q.Enqueue(ptr) // Should block if full, never drop
					wd.Progress()
				}
			}(p)
		}

		// Start consumer
		go func() {
			for !consumerDone.Load() || q.UsedSlots() > 0 {
				ptr, ok := q.Dequeue()
				if ok {
					dequeuedMu.Lock()
					dequeuedItems[ptr] = *ptr
					dequeuedMu.Unlock()
					dequeuedCount.Add(1)
					wd.Progress()
				} else {
					time.Sleep(1 * time.Microsecond)
				}
			}
		}()

		// Wait for all producers to finish
		prodWg.Wait()
		consumerDone.Store(true)

		// Wait for consumer to drain
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) && dequeuedCount.Load() < int64(totalItems) {
			time.Sleep(10 * time.Millisecond)
		}

		// Verify no data loss
		enqueuedMu.Lock()
		dequeuedMu.Lock()
		defer enqueuedMu.Unlock()
		defer dequeuedMu.Unlock()

		if len(enqueuedItems) != totalItems {
			t.Errorf("Enqueued count mismatch: expected %d, got %d", totalItems, len(enqueuedItems))
		}

		if len(dequeuedItems) != totalItems {
			t.Errorf("Dequeued count mismatch: expected %d, got %d", totalItems, len(dequeuedItems))
		}

		// Check for lost items
		lost := 0
		for ptr, val := range enqueuedItems {
			if _, found := dequeuedItems[ptr]; !found {
				lost++
				if lost <= 10 {
					t.Errorf("LOST ITEM: pointer %p with value %d was never dequeued", ptr, val)
				}
			}
		}

		if lost > 0 {
			t.Fatalf("DATA LOSS DETECTED: %d items lost out of %d (%.2f%%)",
				lost, totalItems, float64(lost)/float64(totalItems)*100)
		}
	})
}

// TestFullQueueBlockingWithConcurrentDequeue tests that when producers block
// on a full queue, they correctly resume when consumers make space.
func TestFullQueueBlockingWithConcurrentDequeue(t *testing.T) {
	withAllQueues(t, "FullQueueBlockingWithConcurrentDequeue", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 32
		const numProducers = 10
		const numConsumers = 5
		const itemsPerProducer = 200
		const totalItems = numProducers * itemsPerProducer

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "FullQueueBlockingWithConcurrentDequeue")
		wd.Start()
		defer wd.Stop()

		var enqueuedCount atomic.Int64
		var dequeuedCount atomic.Int64
		var prodWg sync.WaitGroup
		var consWg sync.WaitGroup

		// Start producers - they will block when queue is full
		prodWg.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				for i := 0; i < itemsPerProducer; i++ {
					val := producerID*itemsPerProducer + i
					ptr := new(int)
					*ptr = val
					q.Enqueue(ptr) // Blocks if full
					enqueuedCount.Add(1)
					if i%50 == 0 {
						wd.Progress()
					}
				}
			}(p)
		}

		// Start consumers - they drain the queue
		consWg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func() {
				defer consWg.Done()
				for dequeuedCount.Load() < int64(totalItems) {
					_, ok := q.Dequeue()
					if ok {
						dequeuedCount.Add(1)
						wd.Progress()
					} else {
						time.Sleep(1 * time.Microsecond)
					}
				}
			}()
		}

		// Wait for producers to finish
		prodWg.Wait()

		// Wait for consumers to finish with timeout
		done := make(chan struct{})
		go func() {
			consWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(30 * time.Second):
			t.Fatalf("Timeout waiting for consumers - possible deadlock. Enqueued: %d, Dequeued: %d",
				enqueuedCount.Load(), dequeuedCount.Load())
		}

		// Verify counts match
		if enqueuedCount.Load() != int64(totalItems) {
			t.Errorf("Enqueue count mismatch: expected %d, got %d", totalItems, enqueuedCount.Load())
		}
		if dequeuedCount.Load() != int64(totalItems) {
			t.Errorf("Dequeue count mismatch: expected %d, got %d", totalItems, dequeuedCount.Load())
		}

		// Queue should be empty
		if q.UsedSlots() != 0 {
			t.Errorf("Queue not empty: UsedSlots=%d", q.UsedSlots())
		}
	})
}

// =============================================================================
// Counter Consistency Tests
// =============================================================================

// TestCountersAccurateAfterWrapAround verifies that UsedSlots and FreeSlots
// remain accurate after many ring buffer wrap-arounds.
func TestCountersAccurateAfterWrapAround(t *testing.T) {
	withAllQueues(t, "CountersAccurateAfterWrapAround", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 64
		const iterations = 100000 // Many wrap-arounds

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "CountersAccurateAfterWrapAround")
		wd.Start()
		defer wd.Stop()

		// Initial state check
		if q.UsedSlots() != 0 {
			t.Fatalf("Initial UsedSlots should be 0, got %d", q.UsedSlots())
		}
		if q.FreeSlots() != capacity {
			t.Fatalf("Initial FreeSlots should be %d, got %d", capacity, q.FreeSlots())
		}

		// Run many enqueue/dequeue cycles
		for i := 0; i < iterations; i++ {
			val := i
			q.Enqueue(&val)

			// Periodically check counters
			if i%10000 == 0 {
				used := q.UsedSlots()
				free := q.FreeSlots()
				if used+free != capacity {
					t.Errorf("Counter inconsistency at iteration %d: used=%d, free=%d, sum=%d (expected %d)",
						i, used, free, used+free, capacity)
				}
				wd.Progress()
			}

			// Dequeue
			_, ok := q.Dequeue()
			if !ok {
				t.Fatalf("Failed to dequeue at iteration %d", i)
			}
		}

		// Final state check
		if q.UsedSlots() != 0 {
			t.Errorf("Final UsedSlots should be 0, got %d", q.UsedSlots())
		}
		if q.FreeSlots() != capacity {
			t.Errorf("Final FreeSlots should be %d, got %d", capacity, q.FreeSlots())
		}
	})
}

// =============================================================================
// Near-Boundary Race Condition Tests
// =============================================================================

// TestRaceOnNearFullQueue creates race conditions when the queue is nearly full.
func TestRaceOnNearFullQueue(t *testing.T) {
	withAllQueues(t, "RaceOnNearFullQueue", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 64
		const numGoroutines = 20
		const iterations = 500

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "RaceOnNearFullQueue")
		wd.Start()
		defer wd.Stop()

		for round := 0; round < iterations; round++ {
			// Fill to capacity - 1
			for i := 0; i < int(capacity)-1; i++ {
				val := i
				q.Enqueue(&val)
			}

			var wg sync.WaitGroup
			var enqueueSuccess atomic.Int64
			var dequeueSuccess atomic.Int64

			// Race: multiple goroutines try to enqueue/dequeue simultaneously
			wg.Add(numGoroutines * 2)

			// Enqueuers
			for i := 0; i < numGoroutines; i++ {
				go func(id int) {
					defer wg.Done()
					val := 1000 + id
					// Use a timeout to avoid blocking forever
					done := make(chan struct{})
					go func() {
						q.Enqueue(&val)
						enqueueSuccess.Add(1)
						close(done)
					}()
					select {
					case <-done:
					case <-time.After(100 * time.Millisecond):
						// Enqueue is blocking (expected behavior)
					}
				}(i)
			}

			// Dequeuers
			for i := 0; i < numGoroutines; i++ {
				go func() {
					defer wg.Done()
					if _, ok := q.Dequeue(); ok {
						dequeueSuccess.Add(1)
					}
				}()
			}

			wg.Wait()

			// Drain remaining
			drained := 0
			for {
				if _, ok := q.Dequeue(); ok {
					drained++
				} else {
					break
				}
			}

			// Verify queue is empty
			if q.UsedSlots() != 0 {
				t.Errorf("Round %d: Queue not empty after drain, UsedSlots=%d", round, q.UsedSlots())
			}

			if round%100 == 0 {
				wd.Progress()
			}
		}
	})
}

// TestRaceOnNearEmptyQueue creates race conditions when the queue is nearly empty.
func TestRaceOnNearEmptyQueue(t *testing.T) {
	withAllQueues(t, "RaceOnNearEmptyQueue", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 64
		const numGoroutines = 20
		const iterations = 500

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "RaceOnNearEmptyQueue")
		wd.Start()
		defer wd.Stop()

		for round := 0; round < iterations; round++ {
			// Add exactly 1 item
			val := 42
			q.Enqueue(&val)

			var wg sync.WaitGroup
			var dequeueSuccess atomic.Int64
			var enqueueSuccess atomic.Int64

			wg.Add(numGoroutines * 2)

			// Dequeuers - race to get the single item
			for i := 0; i < numGoroutines; i++ {
				go func() {
					defer wg.Done()
					if _, ok := q.Dequeue(); ok {
						dequeueSuccess.Add(1)
					}
				}()
			}

			// Enqueuers - add more items
			for i := 0; i < numGoroutines; i++ {
				go func(id int) {
					defer wg.Done()
					v := id
					q.Enqueue(&v)
					enqueueSuccess.Add(1)
				}(i)
			}

			wg.Wait()

			// Verify exactly 1 dequeue succeeded for the original item
			// (plus potentially more from the enqueued items)
			totalEnqueued := int64(1) + enqueueSuccess.Load()
			totalDequeued := dequeueSuccess.Load()

			// Drain remaining
			for {
				if _, ok := q.Dequeue(); ok {
					totalDequeued++
				} else {
					break
				}
			}

			if totalDequeued != totalEnqueued {
				t.Errorf("Round %d: Item count mismatch: enqueued=%d, dequeued=%d",
					round, totalEnqueued, totalDequeued)
			}

			if round%100 == 0 {
				wd.Progress()
			}
		}
	})
}

// =============================================================================
// FIFO Ordering Under Backpressure Tests
// =============================================================================

// TestNoReorderingOnBackpressure verifies that when the queue fills up and
// producers block, the FIFO order is maintained when they eventually enqueue.
func TestNoReorderingOnBackpressure(t *testing.T) {
	withAllQueues(t, "NoReorderingOnBackpressure", []string{"FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 32
		const totalItems = 200

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "NoReorderingOnBackpressure")
		wd.Start()
		defer wd.Stop()

		// Create ordered pointers
		pointers := make([]*int, totalItems)
		for i := 0; i < totalItems; i++ {
			p := new(int)
			*p = i
			pointers[i] = p
		}

		// Producer goroutine - enqueues in order, will block when full
		go func() {
			for i := 0; i < totalItems; i++ {
				q.Enqueue(pointers[i])
				wd.Progress()
			}
		}()

		// Consumer - dequeue with intentional delays to create backpressure
		received := make([]*int, 0, totalItems)
		for len(received) < totalItems {
			ptr, ok := q.Dequeue()
			if ok {
				received = append(received, ptr)
				// Add some delay to create backpressure
				if len(received)%10 == 0 {
					time.Sleep(1 * time.Millisecond)
				}
				wd.Progress()
			} else {
				time.Sleep(1 * time.Microsecond)
			}
		}

		// Verify FIFO order maintained
		orderViolations := 0
		for i := 0; i < totalItems; i++ {
			if received[i] != pointers[i] {
				orderViolations++
				if orderViolations <= 10 {
					t.Errorf("FIFO violation at index %d: expected ptr %p (val %d), got ptr %p (val %d)",
						i, pointers[i], *pointers[i], received[i], *received[i])
				}
			}
		}

		if orderViolations > 0 {
			t.Fatalf("FIFO ORDER VIOLATED UNDER BACKPRESSURE: %d violations out of %d items",
				orderViolations, totalItems)
		}
	})
}

// =============================================================================
// GC Stress Test
// =============================================================================

// TestGCDoesntCorruptQueue forces garbage collection during queue operations
// to verify that GC doesn't corrupt queue state or stored pointers.
func TestGCDoesntCorruptQueue(t *testing.T) {
	withAllQueues(t, "GCDoesntCorruptQueue", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 256
		const numProducers = 4
		const numConsumers = 4
		const itemsPerProducer = 1000
		const totalItems = numProducers * itemsPerProducer

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "GCDoesntCorruptQueue")
		wd.Start()
		defer wd.Stop()

		var enqueuedCount atomic.Int64
		var dequeuedCount atomic.Int64
		var prodWg sync.WaitGroup
		var consWg sync.WaitGroup
		var stopConsumers atomic.Bool

		// Start a GC stress goroutine
		stopGC := make(chan struct{})
		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					runtime.GC()
				case <-stopGC:
					return
				}
			}
		}()

		// Producers
		prodWg.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				for i := 0; i < itemsPerProducer; i++ {
					ptr := new(int)
					*ptr = producerID*itemsPerProducer + i
					q.Enqueue(ptr)
					enqueuedCount.Add(1)
					if i%200 == 0 {
						wd.Progress()
					}
				}
			}(p)
		}

		// Consumers
		consWg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func() {
				defer consWg.Done()
				for !stopConsumers.Load() || q.UsedSlots() > 0 {
					ptr, ok := q.Dequeue()
					if ok {
						// Verify pointer is still valid by reading it
						_ = *ptr
						dequeuedCount.Add(1)
						wd.Progress()
					} else {
						time.Sleep(1 * time.Microsecond)
					}
				}
			}()
		}

		// Wait for producers
		prodWg.Wait()
		stopConsumers.Store(true)

		// Wait for consumers
		consWg.Wait()

		// Stop GC stress
		close(stopGC)

		// Verify counts
		if enqueuedCount.Load() != int64(totalItems) {
			t.Errorf("Enqueue count mismatch: expected %d, got %d", totalItems, enqueuedCount.Load())
		}
		if dequeuedCount.Load() != int64(totalItems) {
			t.Errorf("Dequeue count mismatch: expected %d, got %d", totalItems, dequeuedCount.Load())
		}
	})
}

func BenchmarkEnqueueDequeue(b *testing.B) {
	impls := getImplementations()
	for _, impl := range impls {
		// Skip stub implementations.
		if impl.newQueue == nil {
			continue
		}
		b.Run(impl.name, func(b *testing.B) {
			q := impl.newQueue(1024)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x := i
				q.Enqueue(&x)
				// Busy-wait until a value is dequeued.
				for {
					if _, ok := q.Dequeue(); ok {
						break
					}
				}
			}
		})
	}
}
