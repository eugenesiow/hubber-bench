# Hubber Micro Performance Benchmarks (Java)

This is the Java part of the Hubber Micro Performance Benchmark Suite. 

Systems tested are:
* Selective-pub MQTT
* Selective-pub Deepstream
* Selective-sub MQTT
* Selective-sub Deepstream
* Single-threaded RelLogic Selective-pub queue
* Multi-threaded RelLogic Selective-pub queue
* Disruptor Pattern RelLogic Selective-pub queue

Any dataset of the right graph format is supported but we use 2 datasets from Leskovec et al. from Stanford SNAP:
* arXiv General Relativity sparse, undirected collaboration network with 5242 nodes and 14496 edges
* Wikipedia who-votes-on-whom dense, directed network with 7115 nodes and 103689 edges

### Selective-Publish vs Selective-Subscribe Comparison Benchmark

`java -cp hubber-bench-0.0.1-SNAPSHOT.jar uk.ac.soton.ldanalytics.hubber.bench.Publisher 192.168.0.100:6020 1000`

`java -cp hubber-bench-0.0.1-SNAPSHOT.jar uk.ac.soton.ldanalytics.hubber.bench.SmartPublisher 192.168.0.100:6020 1000 graphs/ca-GrQc-ps.txt`

`java -cp hubber-bench-0.0.1-SNAPSHOT.jar uk.ac.soton.ldanalytics.hubber.bench.MQTTPublisher tcp://localhost:1883 1000`

`java -cp hubber-bench-0.0.1-SNAPSHOT.jar uk.ac.soton.ldanalytics.hubber.bench.MQTTSmartPublisher tcp://localhost:1883 1000 graphs/ca-GrQc-ps.txt`

### Selective-Publish RelLogic: Single-threaded, Multi-threaded and Disruptor Benchmark

`java -cp hubber-bench-0.0.1-SNAPSHOT.jar uk.ac.soton.ldanalytics.hubber.bench.SingleThreadedPublisher tcp://localhost:1883 graphs/ca-GrQc-ps.txt 5242 1000`

`java -cp hubber-bench-0.0.1-SNAPSHOT.jar uk.ac.soton.ldanalytics.hubber.bench.MultiThreadedPublisher tcp://localhost:1883 graphs/ca-GrQc-ps.txt 5242 1000`

`java -cp hubber-bench-0.0.1-SNAPSHOT.jar uk.ac.soton.ldanalytics.hubber.bench.DisruptorPublisher tcp://localhost:1883 graphs/ca-GrQc-ps.txt 5242 1000`

### This Project
* [Hubber Bench (Javascript Part)](https://github.com/eugenesiow/hubber-bench-js)
