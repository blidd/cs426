## A4

We ultimately decided to go with an asynchronous "garbage-collection" strategy for expired key-value pairs. Every five seconds, we run a goroutine that iterates through each shard for each node and remove all pairs that have expired.

There were several alternative strategies we considered. One option would be to remove the expired key-value pairs ad-hoc when we process Get() requests for that key and realize that the value is expired. We would return {wasFound: false} and then proceed to delete the key-value pair from the shard. However, if we exclusively employed this strategy to prune expired keys, while it may save us the overhead of running a periodic goroutine in the background, it could result in an enormous build-up of expired keys that never get removed because those keys aren't requested often (e.g. in a "hot-key workload" scenario).

We considered a hybrid approach, both removing keys when we discover they are expired and running an asynchronous garbage collector, however we soon realized that for the sake of simplicity there really was no point in adding the extra complexity of removing keys when a Get() request discovers the key is expired, since the key would be removed within 5 seconds anyway.

Another strategy we considered was a "callback" strategy, spawning a goroutine that would eliminate the key as soon as the TTL expired. However we quickly eliminated this option once we considered the potentially enormous overhead of managing a number of goroutines equal to the number of unique keys. This solution would fail to scale for a large cache, and the memory saved by eliminating keys immediately after they expired would only be outstripped by the memory required to manage the goroutines.

## D2

**Experiment 1**

```
go run cmd/stress/tester.go --shardmap shardmaps/single-node.json --num-keys=1 --get-qps 200
```

 
shardmap: single-node.json

This experiment was simply to see how our cache would respond to a hot key workload, with 1 unique key and a higher get QPS. The results demonstrated our cache performed well, with a 100% correct response rate and reasonable latency.

**Experiment 2**
```
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --set-qps=1000 --get-qps=1
```
shardmap: test-3-node.json

The second experiment we ran was to see how our servers would perform when the write rate is very high and the read rate is low. We ran this experiment using the three-node cluster with five shards to test how our cache would perform in a realistic scenario with multiple nodes and shards distributed across the nodes. We left the number of unique keys at the default size of 1000, and the latency of the writes was still quite low, between 100 and 200 microseconds. Our implementation also had a 100% correct response rate, so it performed very well overall.

**Group work**

Brian: Part A (server storage, TTL, Part C1/C2, Integration Testing, Stress Testing
Seun: Part A (TTL, sharding logic), Integration Testing, Unit Testing
Tony: Part B (client implementation), Part C2/C3, Integration Testing