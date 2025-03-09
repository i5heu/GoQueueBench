package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/i5heu/GoQueueBench/pkg/basicmpmc"
	"github.com/i5heu/GoQueueBench/pkg/buffered"
	"github.com/i5heu/GoQueueBench/pkg/fastmpmc"
	"github.com/i5heu/GoQueueBench/pkg/fastmpmc_ticket"
	"github.com/i5heu/GoQueueBench/pkg/lightningqueue"
	"github.com/i5heu/GoQueueBench/pkg/multiheadqueue"
	"github.com/i5heu/GoQueueBench/pkg/optmpmc"
	"github.com/i5heu/GoQueueBench/pkg/optmpmc_sharded"
	"github.com/i5heu/GoQueueBench/pkg/testbench"
	"github.com/i5heu/GoQueueBench/pkg/vortexqueue"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// BenchmarkResult holds results for one test run.
type BenchmarkResult struct {
	Implementation      string  `json:"implementation"`
	NumProducers        int     `json:"num_producers"`
	NumConsumers        int     `json:"num_consumers"`
	NumMessages         int64   `json:"num_messages"`          // produced count
	NumMessagesConsumed int64   `json:"num_messages_consumed"` // consumed count
	TestDuration        string  `json:"test_duration"`         // e.g. "10s"
	ActualElapsed       string  `json:"actual_elapsed"`        // measured time
	Throughput          float64 `json:"throughput_msgs_sec"`   // based on consumed count
	Timestamp           int64   `json:"timestamp"`
	GoVersion           string  `json:"go_version"`
}

// SystemInfo holds system information.
type SystemInfo struct {
	NumCPU            int     `json:"num_cpu"`
	TrueCPU           int     `json:"true_cpu,omitempty"`
	SimulatedCPUCount int     `json:"simulated_cpu_count,omitempty"`
	CPUModel          string  `json:"cpu_model,omitempty"`
	CPUSpeedMHz       float64 `json:"cpu_speed_mhz,omitempty"`
	GOARCH            string  `json:"go_arch"`
	TotalMemory       uint64  `json:"total_memory_bytes,omitempty"`
}

// FullReport represents a complete test session.
type FullReport struct {
	SessionTime string            `json:"session_time"`
	SystemInfo  SystemInfo        `json:"system_info"`
	Benchmarks  []BenchmarkResult `json:"benchmarks"`
}

// Implementation represents a queue implementation.
type Implementation[T any, Q interface {
	Enqueue(T)
	Dequeue() (T, bool)
	FreeSlots() uint64
	UsedSlots() uint64
}] struct {
	name        string
	description string
	pkgName     string
	authors     []string
	features    []string
	newQueue    func(capacity uint64) Q
}

// outputMarkdownTable loads the JSON file and outputs a Markdown table.
func outputMarkdownTable(jsonFile string) {
	data, err := os.ReadFile(jsonFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading JSON file %q: %v\n", jsonFile, err)
		os.Exit(1)
	}
	var sessions []FullReport
	if err := json.Unmarshal(data, &sessions); err != nil {
		fmt.Fprintf(os.Stderr, "Error unmarshalling JSON: %v\n", err)
		os.Exit(1)
	}
	if len(sessions) == 0 {
		fmt.Fprintln(os.Stderr, "No sessions found in JSON.")
		os.Exit(1)
	}
	// Use the last session for the table.
	lastSession := sessions[len(sessions)-1]
	// Build a map of implementation meta info.
	implMetaMap := make(map[string]Implementation[*int, interface {
		Enqueue(*int)
		Dequeue() (*int, bool)
		FreeSlots() uint64
		UsedSlots() uint64
	}])
	for _, impl := range getImplementations() {
		implMetaMap[impl.name] = impl
	}
	// Build table rows.
	type tableRow struct {
		implementation string
		pkgName        string
		features       string
		author         string
		throughput     float64
	}
	var rows []tableRow
	for _, bench := range lastSession.Benchmarks {
		meta, ok := implMetaMap[bench.Implementation]
		var pkgName, features, authors string
		if ok {
			pkgName = meta.pkgName
			features = strings.Join(meta.features, ", ")
			authors = strings.Join(meta.authors, ", ")
		}
		rows = append(rows, tableRow{
			implementation: bench.Implementation,
			pkgName:        pkgName,
			features:       features,
			author:         authors,
			throughput:     bench.Throughput,
		})
	}
	// Sort rows by throughput descending.
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].throughput > rows[j].throughput
	})
	fmt.Println("## Last Session Benchmark Summary")
	fmt.Println()
	fmt.Println("| Implementation           | Package         | Features                    | Author                      | Throughput (msgs/sec) |")
	fmt.Println("|--------------------------|-----------------|-----------------------------|-----------------------------|-----------------------|")
	for _, r := range rows {
		fmt.Printf("| %-24s | %-15s | %-27s | %-27s | %21.0f |\n",
			r.implementation, r.pkgName, r.features, r.author, r.throughput)
	}
}

// printProgressBar prints a custom progress bar to stderr using \r to overwrite.
func printProgressBar(current, total int, start time.Time) {
	elapsed := time.Since(start)
	avg := time.Duration(0)
	if current > 0 {
		avg = elapsed / time.Duration(current)
	}
	remaining := time.Duration(0)
	if current < total {
		remaining = avg * time.Duration(total-current)
	}
	percent := float64(current) / float64(total) * 100
	barWidth := 20
	filled := int(float64(barWidth) * float64(current) / float64(total))
	bar := strings.Repeat("█", filled) + strings.Repeat(" ", barWidth-filled)
	// Print progress bar with carriage return (without newline)
	fmt.Fprintf(os.Stderr, "\rProgress: %3.0f%% |%s| (%d/%d) ETA: %s", percent, bar, current, total, remaining)
}

func main() {
	// Flags.
	testIterations := flag.Int("iter", 5, "Number of test iterations per concurrency setting")
	cpuMaxFlag := flag.Int("cpu", 0, "If non-zero, test only that GOMAXPROCS value; if 0, test common CPU/vCPU values up to runtime.NumCPU()")
	jsonExport := flag.Bool("json", false, "Export results as JSON to test-results.json")
	highConcurrency := flag.Bool("high-concurrency", false, "Include high concurrency configurations")
	markdownTable := flag.Bool("markdown-table", false, "Output markdown table from test-results.json and exit")
	jsonFileForMarkdown := flag.String("jsonfile", "test-results.json", "Path to JSON file for markdown table")
	progressFlag := flag.Bool("progress", false, "Display a progress bar with ETA")
	flag.Parse()

	if *markdownTable {
		outputMarkdownTable(*jsonFileForMarkdown)
		return
	}

	trueCpuCount := runtime.NumCPU()
	var cpuSettings []int
	// Define the common CPU/vCPU settings.
	commonCPUs := []int{1, 2, 3, 4, 6, 8, 12, 16, 32, 48, 56, 64, 96, 128, 192, 256, 384, 512}

	if *cpuMaxFlag > 0 {
		desired := *cpuMaxFlag
		if desired > trueCpuCount {
			desired = trueCpuCount
		}
		cpuSettings = []int{desired}
	} else {
		for _, v := range commonCPUs {
			if v <= trueCpuCount {
				cpuSettings = append(cpuSettings, v)
			}
		}
	}

	// Define concurrency configurations.
	concurrencyConfigs := []testbench.Config{
		{NumProducers: 2, NumConsumers: 2},
		{NumProducers: 10, NumConsumers: 10},
		{NumProducers: 50, NumConsumers: 50},
	}
	if *highConcurrency {
		concurrencyConfigs = append(concurrencyConfigs,
			testbench.Config{NumProducers: 100, NumConsumers: 100},
			testbench.Config{NumProducers: 250, NumConsumers: 250},
			testbench.Config{NumProducers: 500, NumConsumers: 500},
		)
	}

	// Test duration for each iteration.
	testDuration := 5 * time.Second

	// Calculate total number of tests for progress tracking.
	impls := getImplementations()
	totalTests := len(cpuSettings) * len(concurrencyConfigs) * (*testIterations) * len(impls)
	currentTest := 0
	overallStart := time.Now()

	var allSessions []FullReport

	// Iterate over the desired GOMAXPROCS settings.
	for _, cpus := range cpuSettings {
		runtime.GOMAXPROCS(cpus)
		sysInfo := gatherSystemInfo()
		sysInfo.NumCPU = cpus
		sysInfo.TrueCPU = trueCpuCount
		sysInfo.SimulatedCPUCount = cpus

		// Print CPU header to stdout.
		fmt.Printf("\n=============================\n")
		fmt.Printf("GOMAXPROCS = %d\n", cpus)
		fmt.Printf("=============================\n")

		var results []BenchmarkResult

		// Loop over each concurrency configuration.
		for _, cfg := range concurrencyConfigs {
			fmt.Printf("  [Concurrency: producers=%d, consumers=%d]                                  \n", cfg.NumProducers, cfg.NumConsumers)
			for iteration := 1; iteration <= *testIterations; iteration++ {
				fmt.Printf("    iteration %d/%d                                                      \n", iteration, *testIterations)
				// For each iteration, run each queue implementation.
				for _, impl := range impls {
					runtime.GC()
					q := impl.newQueue(1024)
					time.Sleep(250 * time.Millisecond)

					produced, consumed, actualTime := testbench.RunTimedTest(
						q,
						cfg,
						testDuration,
						func(i int) *int {
							v := i
							return &v
						},
					)
					throughput := float64(consumed) / actualTime.Seconds()
					timestamp := time.Now().Unix()

					currentTest++

					if *progressFlag {
						fmt.Printf("\r")
					}

					// Print test result to stdout.
					fmt.Printf("    %s => produced=%d, consumed=%d, throughput=%.0f msg/s, took=%v\n",
						impl.name, produced, consumed, throughput, actualTime)

					if *progressFlag {
						printProgressBar(currentTest, totalTests, overallStart)
					}

					result := BenchmarkResult{
						Implementation:      impl.name,
						NumProducers:        cfg.NumProducers,
						NumConsumers:        cfg.NumConsumers,
						NumMessages:         produced,
						NumMessagesConsumed: consumed,
						TestDuration:        testDuration.String(),
						ActualElapsed:       actualTime.String(),
						Throughput:          throughput,
						Timestamp:           timestamp,
						GoVersion:           runtime.Version(),
					}
					results = append(results, result)
				}

				if *progressFlag {
					fmt.Printf("\r")
				}
			}
		}

		sessionTime := time.Now().Format(time.RFC3339)
		fr := FullReport{
			SessionTime: sessionTime,
			SystemInfo:  sysInfo,
			Benchmarks:  results,
		}
		allSessions = append(allSessions, fr)
	}

	// After all tests, print a newline so the progress bar line is not overwritten.
	if *progressFlag {
		fmt.Fprintln(os.Stderr)
	}

	// If JSON export is requested, append the new sessions to test-results.json.
	if *jsonExport {
		const filename = "test-results.json"
		var previous []FullReport
		if _, err := os.Stat(filename); err == nil {
			data, err := os.ReadFile(filename)
			if err == nil && len(data) > 0 {
				json.Unmarshal(data, &previous)
			}
		}
		updated := append(previous, allSessions...)
		data, err := json.MarshalIndent(updated, "", "  ")
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error marshalling JSON:", err)
			os.Exit(1)
		}
		if err = os.WriteFile(filename, data, 0644); err != nil {
			fmt.Fprintln(os.Stderr, "Error writing JSON file:", err)
			os.Exit(1)
		}
		fmt.Printf("\nWrote results to %s\n", filename)
	}
}

// gatherSystemInfo collects basic CPU and memory details.
func gatherSystemInfo() SystemInfo {
	numCPU := runtime.NumCPU()
	goArch := runtime.GOARCH

	var cpuModel string
	var cpuSpeed float64
	if infos, err := cpu.Info(); err == nil && len(infos) > 0 {
		cpuModel = infos[0].ModelName
		cpuSpeed = infos[0].Mhz
	}

	var totalMemory uint64
	if vm, err := mem.VirtualMemory(); err == nil {
		totalMemory = vm.Total
	}

	return SystemInfo{
		NumCPU:      numCPU,
		CPUModel:    cpuModel,
		CPUSpeedMHz: cpuSpeed,
		GOARCH:      goArch,
		TotalMemory: totalMemory,
	}
}

// getImplementations enumerates our different queue implementations.
func getImplementations() []Implementation[*int, interface {
	Enqueue(*int)
	Dequeue() (*int, bool)
	FreeSlots() uint64
	UsedSlots() uint64
}] {
	return []Implementation[*int, interface {
		Enqueue(*int)
		Dequeue() (*int, bool)
		FreeSlots() uint64
		UsedSlots() uint64
	}]{
		{
			name:        "Golang Buffered Channel",
			pkgName:     "buffered",
			description: "Works with misused standard go channels, it would be much faster if the go routines would not switch a lot like in this test.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>"},
			features:    []string{"MPMC", "FIFO"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return buffered.New[*int](capacity)
			},
		},
		{
			name:        "BasicMPMCQueue",
			pkgName:     "basicmpmc",
			description: "A basic MPMC queue with no optimizations.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>", "OpenAI o3-mini-high[*](#why-are-there-llms-listed-as-authors)"},
			features:    []string{"MPMC", "FIFO"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return basicmpmc.New[*int](capacity)
			},
		},
		{
			name:        "OptimizedMPMCQueue",
			pkgName:     "optmpmc",
			description: "An optimized MPMC queue with padding to reduce false sharing.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>", "OpenAI o1[*](#why-are-there-llms-listed-as-authors)"},
			features:    []string{"MPMC", "FIFO"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return optmpmc.New[*int](capacity)
			},
		},
		{
			name:        "OptimizedMPMCQueueSharded",
			pkgName:     "optmpmc_sharded",
			description: "An optimized MPMC queue with sharding to reduce contention.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>", "OpenAI o1[*](#why-are-there-llms-listed-as-authors)"},
			features:    []string{"MPMC", "Sharded", "Multi-Head-FIFO"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return optmpmc_sharded.New[*int](capacity)
			},
		},
		{
			name:        "FastMPMCQueue",
			pkgName:     "fastmpmc",
			description: "A highly optimized MPMC queue with cache-friendly memory layout and minimal contention.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>", "OpenAI o3-mini-high[*](#why-are-there-llms-listed-as-authors)"},
			features:    []string{"MPMC", "FIFO", "Cache-Optimized"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return fastmpmc.New[*int](capacity)
			},
		},
		{
			name:        "FastMPMCQueueTicket",
			pkgName:     "fastmpmc_ticket",
			description: "An optimized MPMC queue using a ticket-based reservation system to minimize contention.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>", "OpenAI o3-mini-high[*](#why-are-there-llms-listed-as-authors)"},
			features:    []string{"MPMC", "FIFO", "Cache-Optimized", "Ticket-Based"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return fastmpmc_ticket.New[*int](capacity)
			},
		},
		{
			name:        "MultiHeadQueue",
			pkgName:     "multiheadqueue",
			description: "A sharded, multi-head FIFO queue that reduces contention by dividing capacity among independent ring buffers.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>", "OpenAI o3-mini-high[*](#why-are-there-llms-listed-as-authors)"},
			features:    []string{"MPMC", "Multi-Head-FIFO", "Sharded", "Low Latency"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return multiheadqueue.New[*int](capacity, 0)
			},
		},
		{
			name:        "VortexQueue",
			pkgName:     "vortexqueue",
			description: "An ultra-fast MPMC queue using optimized spin-waiting for reduced contention.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>", "OpenAI o3-mini-high[*](#why-are-there-llms-listed-as-authors)"},
			features:    []string{"MPMC", "FIFO", "Cache-Optimized", "Spin-Wait"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return vortexqueue.New[*int](capacity)
			},
		},
		{
			name:        "LightningQueue",
			pkgName:     "lightningqueue",
			description: "A high-performance MPMC queue optimized for low latency and minimal contention.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>", "OpenAI o3-mini-high[*](#why-are-there-llms-listed-as-authors)"},
			features:    []string{"MPMC", "FIFO", "Cache-Optimized", "Ultra-Low-Latency"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return lightningqueue.New[*int](capacity)
			},
		},
	}
}
