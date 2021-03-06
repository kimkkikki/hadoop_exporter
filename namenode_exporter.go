package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

const (
	namespace = "namenode"
)

var (
	listenAddress  = flag.String("web.listen-address", ":9070", "Address on which to expose metrics and web interface.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	namenodeJmxUrl = flag.String("namenode.jmx.url", "http://localhost:50070/jmx", "Hadoop JMX URL.")
)

type Exporter struct {
	url                      string
	MissingBlocks            prometheus.Gauge
	CapacityTotal            prometheus.Gauge
	CapacityUsed             prometheus.Gauge
	CapacityRemaining        prometheus.Gauge
	CapacityUsedNonDFS       prometheus.Gauge
	BlocksTotal              prometheus.Gauge
	FilesTotal               prometheus.Gauge
	CorruptBlocks            prometheus.Gauge
	ExcessBlocks             prometheus.Gauge
	StaleDataNodes           prometheus.Gauge
	TotalSyncCount           prometheus.Gauge
	heapMemoryUsageCommitted prometheus.Gauge
	heapMemoryUsageInit      prometheus.Gauge
	heapMemoryUsageMax       prometheus.Gauge
	heapMemoryUsageUsed      prometheus.Gauge
	GcCount                  prometheus.Gauge
	GcTimeMillis             prometheus.Gauge
	ThreadsRunnable          prometheus.Gauge
	ThreadsBlocked           prometheus.Gauge
	ThreadsWaiting           prometheus.Gauge
	ThreadsTimedWaiting      prometheus.Gauge
	isActive                 prometheus.Gauge
}

func NewExporter(url string) *Exporter {
	return &Exporter{
		url: url,
		MissingBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "MissingBlocks",
			Help:      "MissingBlocks",
		}),
		CapacityTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CapacityTotal",
			Help:      "CapacityTotal",
		}),
		CapacityUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CapacityUsed",
			Help:      "CapacityUsed",
		}),
		CapacityRemaining: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CapacityRemaining",
			Help:      "CapacityRemaining",
		}),
		CapacityUsedNonDFS: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CapacityUsedNonDFS",
			Help:      "CapacityUsedNonDFS",
		}),
		BlocksTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "BlocksTotal",
			Help:      "BlocksTotal",
		}),
		FilesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "FilesTotal",
			Help:      "FilesTotal",
		}),
		CorruptBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CorruptBlocks",
			Help:      "CorruptBlocks",
		}),
		ExcessBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ExcessBlocks",
			Help:      "ExcessBlocks",
		}),
		StaleDataNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "StaleDataNodes",
			Help:      "StaleDataNodes",
		}),
		TotalSyncCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "TotalSyncCount",
			Help:      "TotalSyncCount",
		}),
		heapMemoryUsageCommitted: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heapMemoryUsageCommitted",
			Help:      "heapMemoryUsageCommitted",
		}),
		heapMemoryUsageInit: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heapMemoryUsageInit",
			Help:      "heapMemoryUsageInit",
		}),
		heapMemoryUsageMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heapMemoryUsageMax",
			Help:      "heapMemoryUsageMax",
		}),
		heapMemoryUsageUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heapMemoryUsageUsed",
			Help:      "heapMemoryUsageUsed",
		}),
		GcCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GcCount",
			Help:      "GcCount",
		}),
		GcTimeMillis: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GcTimeMillis",
			Help:      "GcTimeMillis",
		}),
		ThreadsRunnable: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ThreadsRunnable",
			Help:      "ThreadsRunnable",
		}),
		ThreadsBlocked: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ThreadsBlocked",
			Help:      "ThreadsBlocked",
		}),
		ThreadsWaiting: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ThreadsWaiting",
			Help:      "ThreadsWaiting",
		}),
		ThreadsTimedWaiting: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ThreadsTimedWaiting",
			Help:      "ThreadsTimedWaiting",
		}),
		isActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "isActive",
			Help:      "isActive",
		}),
	}
}

// Describe implements the prometheus.Collector interface.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.MissingBlocks.Describe(ch)
	e.CapacityTotal.Describe(ch)
	e.CapacityUsed.Describe(ch)
	e.CapacityRemaining.Describe(ch)
	e.CapacityUsedNonDFS.Describe(ch)
	e.BlocksTotal.Describe(ch)
	e.FilesTotal.Describe(ch)
	e.CorruptBlocks.Describe(ch)
	e.ExcessBlocks.Describe(ch)
	e.StaleDataNodes.Describe(ch)
	e.TotalSyncCount.Describe(ch)
	e.heapMemoryUsageCommitted.Describe(ch)
	e.heapMemoryUsageInit.Describe(ch)
	e.heapMemoryUsageMax.Describe(ch)
	e.heapMemoryUsageUsed.Describe(ch)
	e.GcCount.Describe(ch)
	e.GcTimeMillis.Describe(ch)
	e.ThreadsRunnable.Describe(ch)
	e.ThreadsBlocked.Describe(ch)
	e.ThreadsWaiting.Describe(ch)
	e.ThreadsTimedWaiting.Describe(ch)
	e.isActive.Describe(ch)
}

// Collect implements the prometheus.Collector interface.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	resp, err := http.Get(e.url)
	if err != nil {
		log.Error(err)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
	}
	var f interface{}
	err = json.Unmarshal(data, &f)
	if err != nil {
		log.Error(err)
	}
	m := f.(map[string]interface{})
	var nameList = m["beans"].([]interface{})
	for _, nameData := range nameList {
		nameDataMap := nameData.(map[string]interface{})
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=FSNamesystem" {
			e.MissingBlocks.Set(nameDataMap["MissingBlocks"].(float64))
			e.CapacityTotal.Set(nameDataMap["CapacityTotal"].(float64))
			e.CapacityUsed.Set(nameDataMap["CapacityUsed"].(float64))
			e.CapacityRemaining.Set(nameDataMap["CapacityRemaining"].(float64))
			e.CapacityUsedNonDFS.Set(nameDataMap["CapacityUsedNonDFS"].(float64))
			e.BlocksTotal.Set(nameDataMap["BlocksTotal"].(float64))
			e.FilesTotal.Set(nameDataMap["FilesTotal"].(float64))
			e.CorruptBlocks.Set(nameDataMap["CorruptBlocks"].(float64))
			e.ExcessBlocks.Set(nameDataMap["ExcessBlocks"].(float64))
			e.StaleDataNodes.Set(nameDataMap["StaleDataNodes"].(float64))
			e.TotalSyncCount.Set(nameDataMap["TotalSyncCount"].(float64))
			if nameDataMap["tag.HAState"] == "active" {
				e.isActive.Set(1)
			} else {
				e.isActive.Set(0)
			}
		}
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=JvmMetrics" {
			e.GcCount.Set(nameDataMap["GcCount"].(float64))
			e.GcTimeMillis.Set(nameDataMap["GcTimeMillis"].(float64))
			e.ThreadsRunnable.Set(nameDataMap["ThreadsRunnable"].(float64))
			e.ThreadsBlocked.Set(nameDataMap["ThreadsBlocked"].(float64))
			e.ThreadsWaiting.Set(nameDataMap["ThreadsWaiting"].(float64))
			e.ThreadsTimedWaiting.Set(nameDataMap["ThreadsTimedWaiting"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=Memory" {
			heapMemoryUsage := nameDataMap["HeapMemoryUsage"].(map[string]interface{})
			e.heapMemoryUsageCommitted.Set(heapMemoryUsage["committed"].(float64))
			e.heapMemoryUsageInit.Set(heapMemoryUsage["init"].(float64))
			e.heapMemoryUsageMax.Set(heapMemoryUsage["max"].(float64))
			e.heapMemoryUsageUsed.Set(heapMemoryUsage["used"].(float64))
		}
	}
	e.MissingBlocks.Collect(ch)
	e.CapacityTotal.Collect(ch)
	e.CapacityUsed.Collect(ch)
	e.CapacityRemaining.Collect(ch)
	e.CapacityUsedNonDFS.Collect(ch)
	e.BlocksTotal.Collect(ch)
	e.FilesTotal.Collect(ch)
	e.CorruptBlocks.Collect(ch)
	e.ExcessBlocks.Collect(ch)
	e.StaleDataNodes.Collect(ch)
	e.TotalSyncCount.Collect(ch)
	e.heapMemoryUsageCommitted.Collect(ch)
	e.heapMemoryUsageInit.Collect(ch)
	e.heapMemoryUsageMax.Collect(ch)
	e.heapMemoryUsageUsed.Collect(ch)
	e.GcCount.Collect(ch)
	e.GcTimeMillis.Collect(ch)
	e.ThreadsRunnable.Collect(ch)
	e.ThreadsBlocked.Collect(ch)
	e.ThreadsWaiting.Collect(ch)
	e.ThreadsTimedWaiting.Collect(ch)
	e.isActive.Collect(ch)
}

func main() {
	flag.Parse()

	exporter := NewExporter(*namenodeJmxUrl)
	prometheus.MustRegister(exporter)

	log.Printf("Starting Server: %s", *listenAddress)
	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>NameNode Exporter</title></head>
		<body>
		<h1>NameNode Exporter</h1>
		<p><a href="` + *metricsPath + `">Metrics</a></p>
		</body>
		</html>`))
	})
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
