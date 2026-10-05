# Kafka and Flink skew

Measured on a fresh job, `d52e60b8761d5178293fdcb4213d62ea`, after `aircraft-telemetry` was raised from 3 partitions to 12 and `parallelism.default` was set to 3. The parallelism-1 savepoint was not restored into this graph. No salting or other mitigation was added. The TaskManager has 3 slots; slot count was not changed.

## Load

60,000 events, 100 aircraft (`AC-S00` through `AC-S99`). `AC-S00` produced 24,000 events, 40% of the batch. The Kafka key is `aircraft_id`, so every event for one aircraft goes to one partition. `event_id` values are unique.

Kafka accepted the batch in 25.2 seconds, 2,385 records/s. The Flink source reached 60,000 records 181.6 seconds after the send started, about 330 records/s average.

## Partition deltas

Counts are the change in log end offset during the send. Historical data still sits on partitions 0–2, so absolute end offsets are not the skew.

| Partition | Records | Share |
| --- | ---: | ---: |
| 0 | 2,546 | 4.2% |
| 1 | 3,275 | 5.5% |
| 2 | 1,815 | 3.0% |
| 3 | 26,545 | 44.2% |
| 4 | 1,816 | 3.0% |
| 5 | 4,002 | 6.7% |
| 6 | 2,182 | 3.6% |
| 7 | 3,636 | 6.1% |
| 8 | 4,725 | 7.9% |
| 9 | 4,366 | 7.3% |
| 10 | 2,546 | 4.2% |
| 11 | 2,546 | 4.2% |

Partition 3 is the hot aircraft plus the other aircraft whose murmur2 hash lands on that partition. 44.2% is a bit above 40% for that reason.

## Flink subtasks

The source assigns partitions round-robin, four per subtask: subtask `i` reads partitions `i`, `i+3`, `i+6`, and `i+9`.

| Subtask | Partitions | Records | Share |
| --- | --- | ---: | ---: |
| 0 | 0, 3, 6, 9 | 35,639 | 59.4% |
| 1 | 1, 4, 7, 10 | 11,273 | 18.8% |
| 2 | 2, 5, 8, 11 | 13,088 | 21.8% |

Subtasks 1 and 2 finished while subtask 0 was still reading partition 3. The source is more skewed than the hottest partition because that partition shares a subtask with three others.

Dedup and the local window aggregate were even: 20,124 / 20,194 / 19,682 (busiest 33.7%). They are chained on `event_id`, which does not collide. `GlobalWindowAggregate`, keyed by aircraft and flight, then received 153 / 96 / 93 records (busiest 44.7%). That is the shuffle after local aggregation, not the raw event stream. Its Iceberg writer was still at 0 because every event shared one open minute, so `aircraft_telemetry_1m` was not part of this measurement.

## Backpressure and throughput

During the 25-second produce, 13 samples of source backpressure and global-window backpressure were all `ok`. This workload showed a long single-subtask tail. It did not show backpressure.

No mitigation was implemented. The imbalance is left as a measured property of keying the topic by `aircraft_id` at 12 partitions and parallelism 3.
