# Hadoop Exporter for Prometheus
Exports hadoop cluster metrics via HTTP for Prometheus consumption.

How to build
```
go get github.com/prometheus/client_golang/prometheus
go get github.com/prometheus/log
go build namenode_exporter.go
go build resourcemanager_exporter.go
go build journalnode_exporter.go
go build datanode_exporter.go
```

Help on flags of namenode_exporter:
```
-namenode.jmx.url string
    Hadoop JMX URL. (default "http://localhost:50070/jmx")
-web.listen-address string
    Address on which to expose metrics and web interface. (default ":9070")
-web.telemetry-path string
    Path under which to expose metrics. (default "/metrics")
```

Help on flags of resourcemanager_exporter:
```
-resourcemanager.url string
    Hadoop ResourceManager URL. (default "http://localhost:8088")
-web.listen-address string
    Address on which to expose metrics and web interface. (default ":9088")
-web.telemetry-path string
    Path under which to expose metrics. (default "/metrics")
```

Help on flags of datanode_exporter:
```
-datanode.jmx.url string
    Hadoop Datanode JMX URL. (default "http://localhost:50075/jmx")
-web.listen-address string
    Address on which to expose metrics and web interface. (default ":9070")
-web.telemetry-path string
    Path under which to expose metrics. (default "/metrics")
```

Help on flags of journalnode_exporter:
```
-journalnode.jmx.url string
    Hadoop Journalnode JMX URL. (default "http://localhost:8480/jmx")
-journalnode.cluster.name string
    Hadoop cluster name. (default "hadoop-cluster")
-web.listen-address string
    Address on which to expose metrics and web interface. (default ":9070")
-web.telemetry-path string
    Path under which to expose metrics. (default "/metrics")
```

Tested on HDP2.8
