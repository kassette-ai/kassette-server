package stats

import (
	"github.com/hashicorp/go-metrics"
	"time"
)

// var writeKeyClientsMap = make(map[string]*statsd.Client)
// var destClientsMap = make(map[string]*statsd.Client)
var statsEnabled bool

// var writeKeyClientsMap = make(map[string]*statsd.Client)
// var destClientsMap = make(map[string]*statsd.Client)

type StatCode int64

const (
	Success StatCode = iota
	Failed
	Disabled
	Waiting
)

func (s StatCode) String() string {
	switch s {
	case Success:
		return "success"
	case Failed:
		return "failed"
	case Disabled:
		return "disabled"
	case Waiting:
		return "waiting"
	}
	return "unknown"
}

var sink metrics.MetricSink

func Init() {
	//config.Initialize()
	//statsEnabled = config.GetBool("enableStats", false)
	statsEnabled = true
	sink = metrics.NewInmemSink(100*time.Millisecond, 60*time.Minute)

	metrics.NewGlobal(metrics.DefaultConfig("kassette-server"), sink)

}

func NewStat(Name string) (kStats *KassetteStats) {
	return &KassetteStats{
		Name: Name,
		Sink: sink,
	}
}

func (kStats *KassetteStats) Increment(name string, count int) {
	kStats.Sink.SetGauge([]string{name}, float32(count))

}

type KassetteStats struct {
	Name     string
	writeKey string
	DestID   string
	StatCode StatCode
	Sink     metrics.MetricSink
}
