package coordinator

import (
	"math"
	"sort"
	"sync"
	"time"

	"gonum.org/v1/gonum/stat"
)

type Model struct {
	id         int
	batch_size int32
	sync.Mutex
	rates         chan float32
	times         chan int64
	processed     int32
	clock         int
	process_times []float64
}

func NewModel(id int, batch int32) Model {
	var times []float64
	m := Model{
		id:            id,
		batch_size:    batch,
		rates:         make(chan float32, 2000),
		times:         make(chan int64, 2000),
		processed:     0,
		clock:         0,
		process_times: times,
	}
	return m
}

func (m *Model) GetId() int {
	return m.id
}

func (m *Model) GetBatchSize() int32 {
	return m.batch_size
}

func (m *Model) GetRate(seconds int) float32 {
	now := time.Now().Unix()
	var sum float32
	for {
		val := <-m.rates
		timestamp := <-m.times
		since := now - timestamp
		if since > int64(seconds) {
			continue
		}
		if since < 0 {
			break
		}
		sum += val
	}
	return sum / float32(seconds)
}

func (m *Model) GetProcessed() int32 {
	return m.processed
}

func (m *Model) GetStats() (float32, float32, float32, []float32) {
	// returns: average, std, median, percentiles (90, 95, 99)
	process_time := m.process_times
	mean := stat.Mean(process_time, nil)

	variance := stat.Variance(process_time, nil)
	stdev := math.Sqrt(variance)

	sort.Float64s(process_time)

	median := stat.Quantile(0.5, stat.Empirical, process_time, nil)
	percentile90 := float32(stat.Quantile(0.9, stat.Empirical, process_time, nil))
	percentile95 := float32(stat.Quantile(0.95, stat.Empirical, process_time, nil))
	percentile99 := float32(stat.Quantile(0.99, stat.Empirical, process_time, nil))

	return float32(mean), float32(stdev), float32(median), []float32{percentile90, percentile95, percentile99}
}

func (m *Model) SetBatchSize(size int32) {
	m.batch_size = size
}

func (m *Model) StartClock() {
	go func() {
		for {
			m.rates <- 0.0
			m.times <- time.Now().Unix()
			time.Sleep(time.Second)
		}
	}()
}

func (m *Model) AddJobs(jobs int) {
	m.Lock()
	m.processed += int32(jobs)
	m.Unlock()
	go func() {
		m.rates <- float32(jobs)
		m.times <- time.Now().Unix()
	}()
}

func (m *Model) AddTime(duration float64) {
	m.process_times = append(m.process_times, duration)
}
