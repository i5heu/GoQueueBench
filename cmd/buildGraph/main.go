package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"image/color"
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
)

// BenchmarkResult holds one benchmark result using the new test schema.
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

// FullReport represents a complete test session using the new scheme.
type FullReport struct {
	SessionTime string            `json:"session_time"`
	SystemInfo  SystemInfo        `json:"system_info"`
	Benchmarks  []BenchmarkResult `json:"benchmarks"`
}

// concurrencyStats holds "5%-avg-min", median, and "5%-avg-max" for each concurrency level.
type concurrencyStats struct {
	concurrency float64 // replaced with category index
	orig        float64 // original concurrency value
	min         float64 // "average of bottom 5%"
	median      float64
	max         float64 // "average of top 5%"
}

// statsPoints implements XYer and YErrorer for concurrencyStats, so we can plot lines + error bars.
type statsPoints []concurrencyStats

func (s statsPoints) Len() int                { return len(s) }
func (s statsPoints) XY(i int) (x, y float64) { return s[i].concurrency, s[i].median }
func (s statsPoints) YError(i int) (low, high float64) {
	low = s[i].median - s[i].min
	high = s[i].max - s[i].median
	return low, high
}

// categoryTicks implements a categorical X-axis: 0,1,2,... => labels for concurrency.
type categoryTicks struct {
	positions []float64
	labels    []string
}

func (ct categoryTicks) Ticks(min, max float64) []plot.Tick {
	var ticks []plot.Tick
	for i, pos := range ct.positions {
		if pos >= min && pos <= max {
			ticks = append(ticks, plot.Tick{Value: pos, Label: ct.labels[i]})
		}
	}
	return ticks
}

// customDenseLogTicks is a custom log tick generator that shows extra minor ticks.
type customDenseLogTicks struct {
	Minor int // how many minor ticks per decade
}

func (t customDenseLogTicks) Ticks(min, max float64) []plot.Tick {
	// Use the standard log tick generator without the unsupported Minor field.
	lt := plot.LogTicks{}
	ticks := lt.Ticks(min, max)
	// Format each major/minor tick label as e.g. "10ns", "100ns", etc.
	for i := range ticks {
		if ticks[i].Label != "" {
			ticks[i].Label = formatNs(ticks[i].Value)
		}
	}
	return ticks
}

func main() {
	jsonFile := flag.String("jsonfile", "test-results.json", "Path to JSON file containing test sessions")
	outputPrefix := flag.String("out", "benchmark_graph", "Output graph image filename prefix")
	flag.Parse()

	data, err := os.ReadFile(*jsonFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading JSON file: %v\n", err)
		os.Exit(1)
	}

	var sessions []FullReport
	if err := json.Unmarshal(data, &sessions); err != nil {
		fmt.Fprintf(os.Stderr, "Error unmarshalling JSON: %v\n", err)
		os.Exit(1)
	}

	// Group data by CPU count -> Implementation -> concurrency -> ns/msg values.
	pointsByCPU := make(map[int]map[string]map[float64][]float64)

	for _, session := range sessions {
		cpus := session.SystemInfo.SimulatedCPUCount
		if cpus == 0 {
			cpus = session.SystemInfo.NumCPU
		}

		if _, ok := pointsByCPU[cpus]; !ok {
			pointsByCPU[cpus] = make(map[string]map[float64][]float64)
		}

		for _, b := range session.Benchmarks {
			x := float64(b.NumProducers + b.NumConsumers)
			dur, err := time.ParseDuration(b.ActualElapsed)
			if err != nil || b.NumMessagesConsumed == 0 {
				continue
			}
			nsPerMsg := float64(dur.Nanoseconds()) / float64(b.NumMessagesConsumed)

			implMap := pointsByCPU[cpus]
			if _, ok := implMap[b.Implementation]; !ok {
				implMap[b.Implementation] = make(map[float64][]float64)
			}
			implMap[b.Implementation][x] = append(implMap[b.Implementation][x], nsPerMsg)
		}
	}

	// For each CPU group, produce a plot.
	for cpus, implMap := range pointsByCPU {
		p := plot.New()
		p.Title.Text = fmt.Sprintf("Benchmark (5%%-avg-min / Median / 5%%-avg-max) vs. Concurrency for %d CPU(s)", cpus)
		p.X.Label.Text = "NumProducers + NumConsumers"
		p.Y.Label.Text = "Time per Msg (ns) [log scale]"
		p.Y.Scale = plot.LinearScale{}

		// Dark theme.
		p.BackgroundColor = color.RGBA{R: 30, G: 30, B: 30, A: 255}
		white := color.RGBA{R: 255, G: 255, B: 255, A: 255}
		p.Title.TextStyle.Color = white
		p.X.Label.TextStyle.Color = white
		p.Y.Label.TextStyle.Color = white
		p.X.Color = white
		p.Y.Color = white
		p.X.Tick.Label.Color = white
		p.Y.Tick.Label.Color = white
		p.Legend.Top = true
		p.Legend.Left = true
		p.Legend.TextStyle.Color = white

		p.Y.Tick.Marker = plot.TickerFunc(func(min, max float64) []plot.Tick {
			// 1) Estimate final height (e.g. 9 inches → 648 px).
			const pxHeight = 648.0
			const pxSpacing = 30.0
			// So about 43 ticks from min to max:
			nTicks := pxHeight / pxSpacing

			// 2) On a log scale, step from log10(min) to log10(max).
			//    (Be sure min > 0, or clamp it, because log10(0) is invalid.)
			if min <= 0 {
				min = 1e-9
			}
			start := math.Log10(min)
			end := math.Log10(max)
			step := (end - start) / nTicks

			var ticks []plot.Tick
			for i := 0.0; i <= nTicks; i++ {
				logVal := start + i*step
				y := math.Pow(10, logVal)
				ticks = append(ticks, plot.Tick{
					Value: y,
					Label: formatNs(y), // same helper you’re already using
				})
			}
			return ticks
		})

		p.Add(plotter.NewGrid())

		// Build union of concurrency values for this CPU group.
		concurrencySet := make(map[float64]struct{})
		for _, implData := range implMap {
			for conc := range implData {
				concurrencySet[conc] = struct{}{}
			}
		}
		var concValues []float64
		for val := range concurrencySet {
			concValues = append(concValues, val)
		}
		sort.Float64s(concValues)

		// Map concurrency => category index.
		concMapping := make(map[float64]float64)
		var positions []float64
		var labels []string
		for i, val := range concValues {
			concMapping[val] = float64(i)
			positions = append(positions, float64(i))
			labels = append(labels, strconv.FormatFloat(val, 'f', -1, 64))
		}
		p.X.Tick.Marker = categoryTicks{positions: positions, labels: labels}

		// Sort implementations alphabetically for consistent legend ordering.
		var implNames []string
		for implName := range implMap {
			implNames = append(implNames, implName)
		}
		sort.Strings(implNames)

		colors := plotutil.SoftColors
		shapes := []draw.GlyphDrawer{
			draw.CircleGlyph{},
			draw.SquareGlyph{},
			draw.TriangleGlyph{},
			draw.CrossGlyph{},
			draw.PlusGlyph{},
		}

		// Slight offset so each implementation is visually separated.
		offsetRange := 0.4
		offsetStep := offsetRange / float64(len(implNames))
		startOffset := -offsetRange/2 + offsetStep/2

		for i, impl := range implNames {
			stats := buildStats(implMap[impl])
			if len(stats) == 0 {
				continue
			}
			for j := range stats {
				baseX := concMapping[stats[j].orig]
				stats[j].concurrency = baseX + startOffset + float64(i)*offsetStep
			}
			sort.Slice(stats, func(a, b int) bool {
				return stats[a].concurrency < stats[b].concurrency
			})
			sp := statsPoints(stats)

			line, err := plotter.NewLine(sp)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating line: %v\n", err)
				continue
			}
			line.Color = colors[i%len(colors)]

			points, err := plotter.NewScatter(sp)
			points.GlyphStyle.Radius = vg.Points(5)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating scatter: %v\n", err)
				continue
			}
			points.Color = colors[i%len(colors)]
			points.Shape = shapes[i%len(shapes)]

			yErrBars, err := plotter.NewYErrorBars(sp)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating error bars: %v\n", err)
				continue
			}
			yErrBars.Color = colors[i%len(colors)]

			p.Add(line, points, yErrBars)
			p.Legend.Add(impl, line, points)
		}

		filename := fmt.Sprintf("%s_%d.png", *outputPrefix, cpus)
		if err := p.Save(12*vg.Inch, 9*vg.Inch, filename); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving plot for %d CPU(s): %v\n", cpus, err)
			continue
		}
		fmt.Printf("Graph for %d CPU(s) saved to %s\n", cpus, filename)
	}
}

// buildStats computes "average of bottom 5%", median, and "average of top 5%".
func buildStats(concurrencyMap map[float64][]float64) []concurrencyStats {
	var out []concurrencyStats
	for x, vals := range concurrencyMap {
		if len(vals) == 0 {
			continue
		}
		sort.Float64s(vals)
		min5 := averageOfRange(vals, 0.0, 0.05)
		max5 := averageOfRange(vals, 0.95, 1.0)
		med := median(vals)

		out = append(out, concurrencyStats{
			concurrency: x,
			orig:        x,
			min:         min5,
			median:      med,
			max:         max5,
		})
	}
	return out
}

// averageOfRange returns the average of sortedVals in [startFrac, endFrac] of its length.
// E.g. averageOfRange(vals, 0, 0.05) is the average of the bottom 5%.
func averageOfRange(sortedVals []float64, startFrac, endFrac float64) float64 {
	n := len(sortedVals)
	if n == 0 {
		return 0
	}
	startIndex := int(float64(n) * startFrac)
	endIndex := int(float64(n) * endFrac)
	if startIndex < 0 {
		startIndex = 0
	}
	if endIndex > n {
		endIndex = n
	}
	if startIndex >= endIndex {
		// fallback to median if 5% slice is too small
		return median(sortedVals)
	}
	sum := 0.0
	for i := startIndex; i < endIndex; i++ {
		sum += sortedVals[i]
	}
	return sum / float64(endIndex-startIndex)
}

func median(sorted []float64) float64 {
	n := len(sorted)
	mid := n / 2
	if n%2 == 1 {
		return sorted[mid]
	}
	return 0.5 * (sorted[mid-1] + sorted[mid])
}

// formatNs nicely formats a nanoseconds value in ns, µs, ms, or s.
func formatNs(ns float64) string {
	switch {
	case ns < 1e3:
		return fmt.Sprintf("%.0fns", ns)
	case ns < 1e6:
		return fmt.Sprintf("%.1fµs", ns/1e3)
	case ns < 1e9:
		return fmt.Sprintf("%.1fms", ns/1e6)
	default:
		return fmt.Sprintf("%.2fs", ns/1e9)
	}
}
