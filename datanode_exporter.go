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
	namespace = "datanode"
)

var (
	listenAddress  = flag.String("web.listen-address", ":9070", "Address on which to expose metrics and web interface.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	datanodeJmxURL = flag.String("datanode.jmx.url", "http://localhost:50075/jmx", "Hadoop Datanode JMX URL.")
)

type DatanodeExporter struct {
	url                        string
	Capacity                   prometheus.Gauge
	DfsUsed                    prometheus.Gauge
	Remaining                  prometheus.Gauge
	NumFailedVolumes           prometheus.Gauge
	LastVolumeFailureDate      prometheus.Gauge
	EstimatedCapacityLostTotal prometheus.Gauge
	CacheUsed                  prometheus.Gauge
	CacheCapacity              prometheus.Gauge
	heapMemoryUsageCommitted   prometheus.Gauge
	heapMemoryUsageInit        prometheus.Gauge
	heapMemoryUsageMax         prometheus.Gauge
	heapMemoryUsageUsed        prometheus.Gauge
	GcCount                    prometheus.Gauge
	GcTimeMillis               prometheus.Gauge
	ThreadsRunnable            prometheus.Gauge
	ThreadsBlocked             prometheus.Gauge
	ThreadsWaiting             prometheus.Gauge
	ThreadsTimedWaiting        prometheus.Gauge
}

func NewDatanodeExporter(url string) *DatanodeExporter {
	return &DatanodeExporter{
		url: url,
		Capacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "Capacity",
			Help:      "Capacity",
		}),
		DfsUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "DfsUsed",
			Help:      "DfsUsed",
		}),
		Remaining: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "Remaining",
			Help:      "Remaining",
		}),
		NumFailedVolumes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "NumFailedVolumes",
			Help:      "NumFailedVolumes",
		}),
		LastVolumeFailureDate: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "LastVolumeFailureDate",
			Help:      "LastVolumeFailureDate",
		}),
		EstimatedCapacityLostTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "EstimatedCapacityLostTotal",
			Help:      "EstimatedCapacityLostTotal",
		}),
		CacheUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CacheUsed",
			Help:      "CacheUsed",
		}),
		CacheCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CacheCapacity",
			Help:      "CacheCapacity",
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
	}
}

// Describe implements the prometheus.Collector interface.
func (e *DatanodeExporter) Describe(ch chan<- *prometheus.Desc) {
	e.Capacity.Describe(ch)
	e.DfsUsed.Describe(ch)
	e.Remaining.Describe(ch)
	e.NumFailedVolumes.Describe(ch)
	e.LastVolumeFailureDate.Describe(ch)
	e.EstimatedCapacityLostTotal.Describe(ch)
	e.CacheUsed.Describe(ch)
	e.CacheCapacity.Describe(ch)
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
}

// Collect implements the prometheus.Collector interface.
func (e *DatanodeExporter) Collect(ch chan<- prometheus.Metric) {
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

		if nameDataMap["name"] == "Hadoop:service=DataNode,name=FSDatasetState" {
			e.Capacity.Set(nameDataMap["Capacity"].(float64))
			e.DfsUsed.Set(nameDataMap["DfsUsed"].(float64))
			e.Remaining.Set(nameDataMap["Remaining"].(float64))
			e.NumFailedVolumes.Set(nameDataMap["NumFailedVolumes"].(float64))
			e.LastVolumeFailureDate.Set(nameDataMap["LastVolumeFailureDate"].(float64))
			e.EstimatedCapacityLostTotal.Set(nameDataMap["EstimatedCapacityLostTotal"].(float64))
			e.CacheUsed.Set(nameDataMap["CacheUsed"].(float64))
			e.CacheCapacity.Set(nameDataMap["CacheCapacity"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=DataNode,name=JvmMetrics" {
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
	e.Capacity.Collect(ch)
	e.DfsUsed.Collect(ch)
	e.Remaining.Collect(ch)
	e.NumFailedVolumes.Collect(ch)
	e.LastVolumeFailureDate.Collect(ch)
	e.EstimatedCapacityLostTotal.Collect(ch)
	e.CacheUsed.Collect(ch)
	e.CacheCapacity.Collect(ch)
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
}

func main() {
	flag.Parse()

	exporter := NewDatanodeExporter(*datanodeJmxURL)
	prometheus.MustRegister(exporter)

	log.Printf("Starting Server: %s", *listenAddress)
	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>DataNode Exporter</title></head>
		<body>
		<h1>DataNode Exporter by <a href="https://github.com/kimkkikki/hadoop_exporter">kimkkikki</a></h1>
		<p><a href="` + *metricsPath + `">Metrics</a></p>
		</body>
		</html>`))
	})
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
