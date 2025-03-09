package testbench

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/i5heu/GoQueueBench/internal/queue"
)

// Config is now only about concurrency: how many producers, how many consumers.
type Config struct {
	NumProducers int
	NumConsumers int
}

// RunTimedTest spawns producers and consumers that run for the specified
// duration, measuring how many messages are actually enqueued/dequeued
// in that window. Once the context expires, producers stop and consumers
// drain any remaining messages in the queue.
// Returns the total messages enqueued, total consumed, and the actual elapsed time.
func RunTimedTest[T any, Q queue.QueueValidationInterface[T]](
	q Q,
	cfg Config,
	testDuration time.Duration,
	valueGenerator func(int) T,
) (producedCount int64, consumedCount int64, elapsed time.Duration) {

	// Create a context that will cancel after testDuration.
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	var totalProduced int64
	var totalConsumed int64

	start := time.Now()

	var msgIndex int64
	var prodWg sync.WaitGroup
	prodWg.Add(cfg.NumProducers)

	// productionDone will be set to 1 when test duration expires.
	var productionDone int32 = 0

	// Launch a goroutine that waits for the test duration to expire and then
	// signals production is done.
	go func() {
		<-ctx.Done()
		atomic.StoreInt32(&productionDone, 1)
	}()

	// Spawn producers.
	for i := 0; i < cfg.NumProducers; i++ {
		go func() {
			defer prodWg.Done()
			// Tight loop that checks the atomic flag.
			for atomic.LoadInt32(&productionDone) == 0 {
				idx := atomic.AddInt64(&msgIndex, 1) - 1
				msg := valueGenerator(int(idx))
				q.Enqueue(msg)
				atomic.AddInt64(&totalProduced, 1)
			}
		}()
	}

	// Spawn consumers.
	for i := 0; i < cfg.NumConsumers; i++ {
		go func() {
			for {
				// If production is done, drain remaining messages.
				if atomic.LoadInt32(&productionDone) == 1 {
					// Drain the queue until empty.
					for {
						if _, ok := q.Dequeue(); ok {
							atomic.AddInt64(&totalConsumed, 1)
						} else {
							break
						}
					}
					return
				}
				// Normal consumption.
				if _, ok := q.Dequeue(); ok {
					atomic.AddInt64(&totalConsumed, 1)
				} else {
					runtime.Gosched()
				}
			}
		}()
	}

	// Wait for the context to expire.
	<-ctx.Done()

	// Wait for all producers to finish.
	prodWg.Wait()

	// Give consumers a short period to drain the remaining messages.
	time.Sleep(100 * time.Millisecond)

	elapsed = time.Since(start)
	producedCount = atomic.LoadInt64(&totalProduced)
	consumedCount = atomic.LoadInt64(&totalConsumed)
	return producedCount, consumedCount, elapsed
}
