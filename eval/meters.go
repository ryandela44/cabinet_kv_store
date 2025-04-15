package eval

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type prioClock = int

type RecordInstance struct {
	StartTime time.Time
	// EndTime can be added if needed.
	TimeElapsed int64
}

type PerfMeter struct {
	sync.RWMutex
	numOfTotalTx   int
	batchSize      int
	sampleInterval prioClock
	lastPClock     prioClock
	fileName       string
	meters         map[prioClock]*RecordInstance
}

func (m *PerfMeter) Init(interval, batchSize int, fileName string) {
	m.sampleInterval = interval
	m.lastPClock = 0
	m.batchSize = batchSize
	m.numOfTotalTx = 0
	m.fileName = fileName
	m.meters = make(map[prioClock]*RecordInstance)
}

func (m *PerfMeter) RecordStarter(pClock prioClock) {
	m.Lock()
	defer m.Unlock()

	m.meters[pClock] = &RecordInstance{
		StartTime:   time.Now(),
		TimeElapsed: 0,
	}
}

func (m *PerfMeter) RecordFinisher(pClock prioClock) error {
	m.Lock()
	defer m.Unlock()

	if _, exist := m.meters[pClock]; !exist {
		return errors.New("pClock has not been recorded with starter")
	}

	start := m.meters[pClock].StartTime
	m.meters[pClock].TimeElapsed = time.Now().Sub(start).Milliseconds()
	return nil
}

// SaveToFile writes a CSV with rows:
// pclock, latency (ms) per batch, throughput (Tx per Second)
// The final row uses pclock "-1" and contains the average latency and overall throughput.
func (m *PerfMeter) SaveToFile() error {
	// Ensure the output directory exists.
	dir := "/Users/macbookpro/GolandProjects/cabinet/eval"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	file, err := os.Create(fmt.Sprintf("%s/%s.csv", dir, m.fileName))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	m.RLock()
	defer m.RUnlock()

	var keys []int
	for key := range m.meters {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	// Write header.
	if err = writer.Write([]string{"pclock", "latency (ms) per batch", "throughput (Tx per Second)"}); err != nil {
		return err
	}

	counter := 0
	var latSum int64 = 0

	for _, key := range keys {
		value := m.meters[key]
		// Only record if a latency was measured.
		if value.TimeElapsed == 0 {
			continue
		}

		latSum += value.TimeElapsed
		counter++

		lat := value.TimeElapsed
		// Throughput (Tx/sec) calculation: number of transactions in the batch divided by (latency in seconds)
		tpt := (float64(m.batchSize) / float64(lat)) * 1000
		row := []string{
			strconv.Itoa(key),
			strconv.FormatInt(lat, 10),
			strconv.FormatFloat(tpt, 'f', 3, 64),
		}
		if err = writer.Write(row); err != nil {
			return err
		}
	}

	if counter == 0 {
		return errors.New("no records to compute averages")
	}

	avgLatency := float64(latSum) / float64(counter)

	// Compute overall elapsed time from the first recorded StartTime to the end of the last record.
	first := m.meters[keys[0]].StartTime
	lastEndTime := m.meters[keys[len(keys)-1]].StartTime.Add(time.Duration(m.meters[keys[len(keys)-1]].TimeElapsed) * time.Millisecond)
	avgThroughput := float64(m.batchSize*counter) / lastEndTime.Sub(first).Seconds()

	// Write final average row with pclock "-1".
	finalRow := []string{
		"-1",
		strconv.FormatFloat(avgLatency, 'f', 3, 64),
		strconv.FormatFloat(avgThroughput, 'f', 3, 64),
	}
	if err = writer.Write(finalRow); err != nil {
		return err
	}

	return nil
}

// ------------------------
// Additional Code for Comparison
// ------------------------

// PerformanceMetrics holds the parsed average metrics from a CSV.
type PerformanceMetrics struct {
	AvgLatency float64 // in milliseconds
	Throughput float64 // transactions per second
}

// parsePerformanceMetrics reads a CSV file produced by SaveToFile()
// and extracts the final row (where pclock == -1) as the average metrics.
func parsePerformanceMetrics(filePath string) (*PerformanceMetrics, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var avgRow []string
	// Find the row where the first column is "-1".
	for _, row := range rows {
		if len(row) > 0 && row[0] == "-1" {
			avgRow = row
			break
		}
	}
	if avgRow == nil || len(avgRow) < 3 {
		return nil, errors.New("average row with pclock '-1' not found or incomplete in the CSV")
	}

	avgLatency, err := strconv.ParseFloat(avgRow[1], 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing average latency: %v", err)
	}

	throughput, err := strconv.ParseFloat(avgRow[2], 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing throughput: %v", err)
	}

	return &PerformanceMetrics{
		AvgLatency: avgLatency,
		Throughput: throughput,
	}, nil
}

// ComparePerformanceMetrics reads two CSV files and prints a comparison
// of the average latency and throughput.
func ComparePerformanceMetrics(file1, file2 string) error {
	metrics1, err := parsePerformanceMetrics(file1)
	if err != nil {
		return fmt.Errorf("error parsing file %s: %v", file1, err)
	}
	metrics2, err := parsePerformanceMetrics(file2)
	if err != nil {
		return fmt.Errorf("error parsing file %s: %v", file2, err)
	}

	fmt.Println("Performance Comparison:")
	fmt.Printf("File: %s -> Average Latency: %.3f ms, Throughput: %.3f Tx/s\n", file1, metrics1.AvgLatency, metrics1.Throughput)
	fmt.Printf("File: %s -> Average Latency: %.3f ms, Throughput: %.3f Tx/s\n", file2, metrics2.AvgLatency, metrics2.Throughput)

	// Compute differences (file1 minus file2).
	diffLatency := metrics1.AvgLatency - metrics2.AvgLatency
	diffThroughput := metrics1.Throughput - metrics2.Throughput
	fmt.Printf("Latency Difference: %.3f ms (positive means file1 is slower)\n", diffLatency)
	fmt.Printf("Throughput Difference: %.3f Tx/s (positive means file1 has higher throughput)\n", diffThroughput)

	// Optionally, compute percentage differences if metric values are non-zero.
	if metrics2.AvgLatency != 0 {
		percentLatency := (diffLatency / metrics2.AvgLatency) * 100
		fmt.Printf("Percentage Latency Difference: %.2f%%\n", percentLatency)
	}
	if metrics2.Throughput != 0 {
		percentThroughput := (diffThroughput / metrics2.Throughput) * 100
		fmt.Printf("Percentage Throughput Difference: %.2f%%\n", percentThroughput)
	}

	return nil
}
