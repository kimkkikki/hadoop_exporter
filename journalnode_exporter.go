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
	namespace = "journalnode"
)

var (
	listenAddress     = flag.String("web.listen-address", ":9070", "Address on which to expose metrics and web interface.")
	metricsPath       = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	journalnodeJmxURL = flag.String("journalnode.jmx.url", "http://localhost:8480/jmx", "Hadoop journalnode JMX URL.")
	clusterName       = flag.String("journalnode.cluster.name", "hadoop-cluster", "Hadoop Cluster Name")
)

type JournalnodeExporter struct {
	url                        string
	clusterName                string
	SyncsNumOps                prometheus.Gauge
	BatchesWritten             prometheus.Gauge
	TxnsWritten                prometheus.Gauge
	BytesWritten               prometheus.Gauge
	BatchesWrittenWhileLagging prometheus.Gauge
	LastWrittenTxId            prometheus.Gauge
	LastPromisedEpoch          prometheus.Gauge
	LastWriterEpoch            prometheus.Gauge
	LastJournalTimestamp       prometheus.Gauge
	CurrentLagTxns             prometheus.Gauge
	GcCount                    prometheus.Gauge
	GcTimeMillis               prometheus.Gauge
	ThreadsRunnable            prometheus.Gauge
	ThreadsBlocked             prometheus.Gauge
	ThreadsWaiting             prometheus.Gauge
	ThreadsTimedWaiting        prometheus.Gauge
	heapMemoryUsageCommitted   prometheus.Gauge
	heapMemoryUsageInit        prometheus.Gauge
	heapMemoryUsageMax         prometheus.Gauge
	heapMemoryUsageUsed        prometheus.Gauge
}

func NewJournalnodeExporter(url string, clusterName string) *JournalnodeExporter {
	return &JournalnodeExporter{
		url:         url,
		clusterName: clusterName,
		SyncsNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "SyncsNumOps",
			Help:      "SyncsNumOps",
		}),
		BatchesWritten: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "BatchesWritten",
			Help:      "BatchesWritten",
		}),
		TxnsWritten: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "TxnsWritten",
			Help:      "TxnsWritten",
		}),
		BytesWritten: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "BytesWritten",
			Help:      "BytesWritten",
		}),
		BatchesWrittenWhileLagging: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "BatchesWrittenWhileLagging",
			Help:      "BatchesWrittenWhileLagging",
		}),
		LastWrittenTxId: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "LastWrittenTxId",
			Help:      "LastWrittenTxId",
		}),
		LastPromisedEpoch: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "LastPromisedEpoch",
			Help:      "LastPromisedEpoch",
		}),
		LastWriterEpoch: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "LastWriterEpoch",
			Help:      "LastWriterEpoch",
		}),
		LastJournalTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "LastJournalTimestamp",
			Help:      "LastJournalTimestamp",
		}),
		CurrentLagTxns: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CurrentLagTxns",
			Help:      "CurrentLagTxns",
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
	}
}

// Describe implements the prometheus.Collector interface.
func (e *JournalnodeExporter) Describe(ch chan<- *prometheus.Desc) {
	e.SyncsNumOps.Describe(ch)
	e.BatchesWritten.Describe(ch)
	e.TxnsWritten.Describe(ch)
	e.BytesWritten.Describe(ch)
	e.BatchesWrittenWhileLagging.Describe(ch)
	e.LastWrittenTxId.Describe(ch)
	e.LastPromisedEpoch.Describe(ch)
	e.LastWriterEpoch.Describe(ch)
	e.LastJournalTimestamp.Describe(ch)
	e.CurrentLagTxns.Describe(ch)
	e.GcCount.Describe(ch)
	e.GcTimeMillis.Describe(ch)
	e.ThreadsRunnable.Describe(ch)
	e.ThreadsBlocked.Describe(ch)
	e.ThreadsWaiting.Describe(ch)
	e.ThreadsTimedWaiting.Describe(ch)
	e.heapMemoryUsageCommitted.Describe(ch)
	e.heapMemoryUsageInit.Describe(ch)
	e.heapMemoryUsageMax.Describe(ch)
	e.heapMemoryUsageUsed.Describe(ch)
}

// Collect implements the prometheus.Collector interface.
func (e *JournalnodeExporter) Collect(ch chan<- prometheus.Metric) {
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
		clusterNameStr := *clusterName
		if nameDataMap["name"] == "Hadoop:service=JournalNode,name=Journal-"+clusterNameStr {
			e.SyncsNumOps.Set(nameDataMap["Syncs60sNumOps"].(float64))
			e.BatchesWritten.Set(nameDataMap["BatchesWritten"].(float64))
			e.TxnsWritten.Set(nameDataMap["TxnsWritten"].(float64))
			e.BytesWritten.Set(nameDataMap["BytesWritten"].(float64))
			e.BatchesWrittenWhileLagging.Set(nameDataMap["BatchesWrittenWhileLagging"].(float64))
			e.LastWrittenTxId.Set(nameDataMap["LastWrittenTxId"].(float64))
			e.LastPromisedEpoch.Set(nameDataMap["LastPromisedEpoch"].(float64))
			e.LastWriterEpoch.Set(nameDataMap["LastWriterEpoch"].(float64))
			e.LastJournalTimestamp.Set(nameDataMap["LastJournalTimestamp"].(float64))
			e.CurrentLagTxns.Set(nameDataMap["CurrentLagTxns"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=JournalNode,name=JvmMetrics" {
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
	e.SyncsNumOps.Collect(ch)
	e.BatchesWritten.Collect(ch)
	e.TxnsWritten.Collect(ch)
	e.BytesWritten.Collect(ch)
	e.BatchesWrittenWhileLagging.Collect(ch)
	e.LastWrittenTxId.Collect(ch)
	e.LastPromisedEpoch.Collect(ch)
	e.LastWriterEpoch.Collect(ch)
	e.LastJournalTimestamp.Collect(ch)
	e.CurrentLagTxns.Collect(ch)
	e.GcCount.Collect(ch)
	e.GcTimeMillis.Collect(ch)
	e.ThreadsRunnable.Collect(ch)
	e.ThreadsBlocked.Collect(ch)
	e.ThreadsWaiting.Collect(ch)
	e.ThreadsTimedWaiting.Collect(ch)
	e.heapMemoryUsageCommitted.Collect(ch)
	e.heapMemoryUsageInit.Collect(ch)
	e.heapMemoryUsageMax.Collect(ch)
	e.heapMemoryUsageUsed.Collect(ch)
}

func main() {
	flag.Parse()

	exporter := NewJournalnodeExporter(*journalnodeJmxURL, *clusterName)
	prometheus.MustRegister(exporter)

	log.Printf("Starting Server: %s", *listenAddress)
	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>Journalnode Exporter</title></head>
		<body>
		<h1>Journalnode Exporter by <a href="https://github.com/kimkkikki/hadoop_exporter">kimkkikki</a></h1>
		<p><a href="` + *metricsPath + `">Metrics</a></p>
		</body>
		</html>`))
	})
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
