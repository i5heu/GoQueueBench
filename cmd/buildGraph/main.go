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

// LinearThenLogScale splits the axis at s.Break.
// Below Break is linear, above Break is log.
type LinearThenLogScale struct {
	Break float64
}

// Normalize maps data [min..max] to [0..1] on the plot.
// We devote the bottom half of the axis (0..0.5) to linear space [min..Break],
// and the top half (0.5..1.0) to log space [Break..max].
func (s LinearThenLogScale) Normalize(domainMin, domainMax, v float64) float64 {
	if domainMin < 0 {
		domainMin = 0
	}
	if domainMax <= s.Break {
		domainMax = s.Break + 1
	}

	linMin := domainMin
	linMax := s.Break

	logMin := math.Log10(s.Break)
	logMax := math.Log10(domainMax)

	if v <= s.Break {
		frac := (v - linMin) / (linMax - linMin)
		return 0.5 * frac
	}
	logV := math.Log10(v)
	frac := (logV - logMin) / (logMax - logMin)
	return 0.5 + 0.5*frac
}

// Inverse maps normalized [0..1] back to the data domain [min..max].
func (s LinearThenLogScale) Inverse(domainMin, domainMax float64) func(float64) float64 {
	if domainMin < 0 {
		domainMin = 0
	}
	if domainMax <= s.Break {
		domainMax = s.Break + 1
	}

	linMin := domainMin
	linMax := s.Break

	logMin := math.Log10(s.Break)
	logMax := math.Log10(domainMax)

	return func(norm float64) float64 {
		if norm <= 0.5 {
			frac := norm / 0.5
			return linMin + frac*(linMax-linMin)
		}
		frac := (norm - 0.5) / 0.5
		logVal := logMin + frac*(logMax-logMin)
		return math.Pow(10, logVal)
	}
}

func main() {
	jsonFile := flag.String("jsonfile", "test-results.json", "Path to JSON file containing test sessions")
	outputPrefix := flag.String("out", "benchmark_graph", "Output graph image filename prefix")
	flag.Parse()

	// Create the output directory ".benches" if it doesn't exist.
	err := os.MkdirAll(".benches", 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating .benches directory: %v\n", err)
		os.Exit(1)
	}
	// Force saving images in .benches.
	*outputPrefix = ".benches/benchmark_graph"

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

	// Prepare sorted CPU groups.
	var cpuGroups []int
	for cpus := range pointsByCPU {
		cpuGroups = append(cpuGroups, cpus)
	}
	sort.Ints(cpuGroups)

	// Loop over each CPU group, generating both log and linear plots.
	for _, cpus := range cpuGroups {
		implMap := pointsByCPU[cpus]

		// Compute maximum ns/msg from the data.
		maxData := 0.0
		for _, implData := range implMap {
			for _, vals := range implData {
				for _, v := range vals {
					if v > maxData {
						maxData = v
					}
				}
			}
		}
		yMax := maxData * 1.1

		// Build union of concurrency values.
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

		// Compute break value: 2 ns above the maximum ns/msg for the smallest concurrency level.
		var breakValue float64
		if len(concValues) > 0 {
			firstConc := concValues[0]
			maxForFirstConc := 0.0
			for _, implData := range implMap {
				if vals, ok := implData[firstConc]; ok {
					for _, v := range vals {
						if v > maxForFirstConc {
							maxForFirstConc = v
						}
					}
				}
			}
			breakValue = maxForFirstConc + 2
		} else {
			breakValue = 100
		}

		// Map concurrency values to category positions.
		concMapping := make(map[float64]float64)
		var positions []float64
		var labels []string
		for i, val := range concValues {
			concMapping[val] = float64(i)
			positions = append(positions, float64(i))
			labels = append(labels, strconv.FormatFloat(val, 'f', -1, 64))
		}

		// Sort implementations alphabetically.
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

		// Offset for visual separation.
		offsetRange := 0.4
		offsetStep := offsetRange / float64(len(implNames))
		startOffset := -offsetRange/2 + offsetStep/2

		// ===== Generate Log Scale Plot =====
		pLog := plot.New()
		pLog.Title.Text = fmt.Sprintf("Benchmark (5%%-avg-min / Median / 5%%-avg-max) vs. Concurrency for %d CPU(s) [Log Scale]", cpus)
		pLog.X.Label.Text = "NumProducers + NumConsumers"
		pLog.Y.Label.Text = "Time per Msg (ns) [log scale]"
		pLog.Y.Min = 0
		pLog.Y.Max = yMax
		pLog.Y.Scale = LinearThenLogScale{Break: breakValue}

		// Dark theme.
		pLog.BackgroundColor = color.RGBA{30, 30, 30, 255}
		white := color.RGBA{255, 255, 255, 255}
		pLog.Title.TextStyle.Color = white
		pLog.X.Label.TextStyle.Color = white
		pLog.Y.Label.TextStyle.Color = white
		pLog.X.Color = white
		pLog.Y.Color = white
		pLog.X.Tick.Label.Color = white
		pLog.Y.Tick.Label.Color = white
		pLog.Legend.Top = true
		pLog.Legend.Left = true
		pLog.Legend.TextStyle.Color = white

		// Custom tick marker for log scale.
		pLog.Y.Tick.Marker = plot.TickerFunc(func(min, max float64) []plot.Tick {
			const pxHeight = 648.0
			const pxSpacing = 25.0
			nTicks := pxHeight / pxSpacing

			linLog, ok := pLog.Y.Scale.(LinearThenLogScale)
			if !ok {
				fmt.Fprintf(os.Stderr, "Error: pLog.Y.Scale is not of type LinearThenLogScale\n")
				return nil
			}
			scaleInv := linLog.Inverse(min, max)
			var ticks []plot.Tick
			for i := 0.0; i <= nTicks; i++ {
				frac := i / nTicks
				dataVal := scaleInv(frac)
				if dataVal < 0 {
					continue
				}
				ticks = append(ticks, plot.Tick{Value: dataVal, Label: formatNs(dataVal)})
			}
			return ticks
		})

		pLog.Add(plotter.NewGrid())

		// Add series for each implementation.
		for i, impl := range implNames {
			stats := buildStats(implMap[impl])
			if len(stats) == 0 {
				continue
			}
			for j := range stats {
				baseX := concMapping[stats[j].orig]
				stats[j].concurrency = baseX + startOffset + float64(i)*offsetStep
			}
			sort.Slice(stats, func(a, b int) bool { return stats[a].concurrency < stats[b].concurrency })
			sp := statsPoints(stats)
			line, err := plotter.NewLine(sp)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating line: %v\n", err)
				continue
			}
			line.Color = colors[i%len(colors)]
			points, err := plotter.NewScatter(sp)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating scatter: %v\n", err)
				continue
			}
			points.GlyphStyle.Radius = vg.Points(5)
			points.Color = colors[i%len(colors)]
			points.Shape = shapes[i%len(shapes)]
			yErrBars, err := plotter.NewYErrorBars(sp)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating error bars: %v\n", err)
				continue
			}
			yErrBars.Color = colors[i%len(colors)]
			pLog.Add(line, points, yErrBars)
			pLog.Legend.Add(impl, line, points)
		}
		pLog.X.Tick.Marker = categoryTicks{positions: positions, labels: labels}
		filenameLog := fmt.Sprintf("%s_%d.png", *outputPrefix, cpus)
		if err := pLog.Save(12*vg.Inch, 9*vg.Inch, filenameLog); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving log plot for %d CPU(s): %v\n", cpus, err)
		} else {
			fmt.Printf("Log plot for %d CPU(s) saved to %s\n", cpus, filenameLog)
		}

		// ===== Generate Linear Scale Plot =====
		pLin := plot.New()
		pLin.Title.Text = fmt.Sprintf("Benchmark (5%%-avg-min / Median / 5%%-avg-max) vs. Concurrency for %d CPU(s) [Linear Scale]", cpus)
		pLin.X.Label.Text = "NumProducers + NumConsumers"
		pLin.Y.Label.Text = "Time per Msg (ns) [linear scale]"
		pLin.Y.Min = 0
		pLin.Y.Max = yMax
		// For linear scale, we use the default scale.

		pLin.BackgroundColor = color.RGBA{30, 30, 30, 255}
		pLin.Title.TextStyle.Color = white
		pLin.X.Label.TextStyle.Color = white
		pLin.Y.Label.TextStyle.Color = white
		pLin.X.Color = white
		pLin.Y.Color = white
		pLin.X.Tick.Label.Color = white
		pLin.Y.Tick.Label.Color = white
		pLin.Legend.Top = true
		pLin.Legend.Left = true
		pLin.Legend.TextStyle.Color = white
		// For the linear plot, we create a custom TickerFunc so that
		// each tick is labeled in "ns" (or µs, ms, etc.) and we get
		// more frequent horizontal lines.
		pLin.Y.Tick.Marker = plot.TickerFunc(func(min, max float64) []plot.Tick {
			// We can aim for about 12–15 ticks.
			const nTicks = 15.0
			if max <= min {
				return nil
			}
			step := (max - min) / (nTicks - 1)

			var ticks []plot.Tick
			for i := 0; i < int(nTicks); i++ {
				val := min + float64(i)*step
				// If negative times don’t make sense, skip them:
				if val < 0 {
					continue
				}
				ticks = append(ticks, plot.Tick{
					Value: val,
					Label: formatNs(val), // e.g. "12ns", "100µs", etc.
				})
			}
			return ticks
		})

		// For linear scale, default tick marker is sufficient.
		pLin.Add(plotter.NewGrid())

		// Add series for each implementation (same as above).
		for i, impl := range implNames {
			stats := buildStats(implMap[impl])
			if len(stats) == 0 {
				continue
			}
			for j := range stats {
				baseX := concMapping[stats[j].orig]
				stats[j].concurrency = baseX + startOffset + float64(i)*offsetStep
			}
			sort.Slice(stats, func(a, b int) bool { return stats[a].concurrency < stats[b].concurrency })
			sp := statsPoints(stats)
			line, err := plotter.NewLine(sp)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating line: %v\n", err)
				continue
			}
			line.Color = colors[i%len(colors)]
			points, err := plotter.NewScatter(sp)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating scatter: %v\n", err)
				continue
			}
			points.GlyphStyle.Radius = vg.Points(5)
			points.Color = colors[i%len(colors)]
			points.Shape = shapes[i%len(shapes)]
			yErrBars, err := plotter.NewYErrorBars(sp)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating error bars: %v\n", err)
				continue
			}
			yErrBars.Color = colors[i%len(colors)]
			pLin.Add(line, points, yErrBars)
			pLin.Legend.Add(impl, line, points)
		}
		pLin.X.Tick.Marker = categoryTicks{positions: positions, labels: labels}
		filenameLin := fmt.Sprintf("%s_%d_linear.png", *outputPrefix, cpus)
		if err := pLin.Save(12*vg.Inch, 9*vg.Inch, filenameLin); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving linear plot for %d CPU(s): %v\n", cpus, err)
		} else {
			fmt.Printf("Linear plot for %d CPU(s) saved to %s\n", cpus, filenameLin)
		}
	}

	// ===== Output Markdown Table =====
	fmt.Println("\nMarkdown Table:")
	fmt.Println()
	fmt.Println("| Cores | Log Scale | Linear scale |")
	fmt.Println("|-------|-----------|--------------|")
	for _, cpus := range cpuGroups {
		row := fmt.Sprintf("| %d Cores | ![Log Scale](.benches/benchmark_graph_%d.png) | ![Linear scale](.benches/benchmark_graph_%d_linear.png) |", cpus, cpus, cpus)
		fmt.Println(row)
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
