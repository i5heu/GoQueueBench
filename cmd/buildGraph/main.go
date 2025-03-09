package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"image/color"
	"os"
	"sort"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

// BenchmarkResult holds one benchmark result using the new test schema.
type BenchmarkResult struct {
	Implementation      string  `json:"implementation"`
	NumProducers        int     `json:"num_producers"`
	NumConsumers        int     `json:"num_consumers"`
	NumMessages         int64   `json:"num_messages"`
	NumMessagesConsumed int64   `json:"num_messages_consumed"`
	TestDuration        string  `json:"test_duration"`
	ActualElapsed       string  `json:"actual_elapsed"`
	Throughput          float64 `json:"throughput_msgs_sec"`
	Timestamp           int64   `json:"timestamp"`
	GoVersion           string  `json:"go_version"`
}

// FullReport represents one test session.
type FullReport struct {
	SessionTime string            `json:"session_time"` // human-readable session start time
	Benchmarks  []BenchmarkResult `json:"benchmarks"`
}

func main() {
	// Optional flags.
	jsonFile := flag.String("jsonfile", "test-results.json", "Path to JSON file containing test sessions")
	output := flag.String("out", "benchmark_graph.png", "Output graph image filename")
	flag.Parse()

	// Load all test sessions from JSON.
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

	// Group data: for each Implementation, we collect (X,Y) points:
	//   X = (NumProducers + NumConsumers)
	//   Y = (ActualElapsed in ns) / (NumMessagesConsumed)
	pointsByImpl := make(map[string]plotter.XYs)

	for _, session := range sessions {
		for _, res := range session.Benchmarks {
			x := float64(res.NumProducers + res.NumConsumers)

			// Parse the actual elapsed time.
			dur, err := time.ParseDuration(res.ActualElapsed)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error parsing ActualElapsed '%s': %v\n", res.ActualElapsed, err)
				continue
			}

			// Avoid division by zero.
			if res.NumMessagesConsumed == 0 {
				continue
			}
			nsPerMsg := float64(dur.Nanoseconds()) / float64(res.NumMessagesConsumed)

			pointsByImpl[res.Implementation] = append(pointsByImpl[res.Implementation],
				plotter.XY{X: x, Y: nsPerMsg})
		}
	}

	// Sort each implementation's points by X so the lines are smooth.
	for _, pts := range pointsByImpl {
		sort.Slice(pts, func(i, j int) bool {
			return pts[i].X < pts[j].X
		})
	}

	// Create the plot.
	p := plot.New()
	p.Title.Text = "Benchmark Average Time per Message vs. (Producers + Consumers)"
	p.X.Label.Text = "NumProducers + NumConsumers"
	p.Y.Label.Text = "Avg Time per Msg (ns) [log scale]"

	// Apply a dark theme.
	darkGray := color.RGBA{R: 30, G: 30, B: 30, A: 255}
	white := color.RGBA{R: 255, G: 255, B: 255, A: 255}

	p.BackgroundColor = darkGray
	p.Title.TextStyle.Color = white
	p.X.Label.TextStyle.Color = white
	p.Y.Label.TextStyle.Color = white
	p.X.Color = white
	p.Y.Color = white
	p.X.Tick.Label.Color = white
	p.Y.Tick.Label.Color = white
	p.X.Tick.LineStyle.Color = white
	p.Y.Tick.LineStyle.Color = white

	// Place legend at the top, horizontal.
	p.Legend.Top = true
	p.Legend.Left = true
	p.Legend.TextStyle.Color = white
	p.Legend.ThumbnailWidth = vg.Points(8)

	// Use a logarithmic scale for the Y-axis with a custom tick marker.
	p.Y.Scale = plot.LogScale{}
	p.Y.Tick.Marker = customNsLogTicks{}

	// Build line data for each implementation.
	var items []interface{}
	for impl, pts := range pointsByImpl {
		items = append(items, impl, pts)
	}

	// Add the lines to the plot.
	if err := plotutil.AddLinePoints(p, items...); err != nil {
		fmt.Fprintf(os.Stderr, "Error adding line points: %v\n", err)
		os.Exit(1)
	}

	// Save the plot as a PNG image.
	if err := p.Save(12*vg.Inch, 9*vg.Inch, *output); err != nil {
		fmt.Fprintf(os.Stderr, "Error saving plot: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Graph saved to %s\n", *output)
}

// customNsLogTicks wraps the built-in log ticks and formats the labels as "XXXns", "XXXµs", "XXXms", or "XXXs".
type customNsLogTicks struct{}

func (customNsLogTicks) Ticks(min, max float64) []plot.Tick {
	ticks := plot.LogTicks{}.Ticks(min, max)
	for i := range ticks {
		if ticks[i].Label == "" {
			continue
		}
		val := ticks[i].Value
		ticks[i].Label = formatNs(val)
	}
	return ticks
}

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
