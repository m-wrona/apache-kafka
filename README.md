# apache-kafka

Modified samples from [Confluent](https://github.com/confluentinc/examples) in order to play with new features of Kafka a bit.

## API

**GET** /config/instances: get current instances of Kafka

**GET** /config/instances/{storeName}: get info about store in Kafka instances
 
**GET** /music/charts/genre/{type}: get info about chosen type of music

**GET** /music/charts/top-five: get top five songs

**GET** /music/song/{id}: get info about chosen song
 
## Development

Pre-requisites:

* docker-compose

* Java

* Maven

1) Build project

```
mvn package
```

2) Run locally

First start Kafka and data generator by executing:

```
docker-compose up
```

and then start your web server:

```
./run_web.sh
```

## Kafka

You can run Kafka and related stuff for demo purposes using following command:

```
docker-compose up
```

Other commands:

```
kafka-avro-console-consumer --topic song-feed --bootstrap-server localhost:9092 --from-beginning
```

```
kafka-topics --list --zookeeper localhost:32181
```

## KSQL

KSQL will be started automatically after running `docker-compose`.

1) Access KSQL server

```
docker-compose exec ksql-cli ksql-cli local --bootstrap-server kafka:29092
```

#### KSQL - manual sample

Sample like 'top five' can also be delivered using KSQL. In order to do that do the following:

1) Create `KSQL` table

```sql
CREATE STREAM s_play_events \
  (song_id LONG, \
    duration LONG) \
  WITH (KAFKA_TOPIC = 'play-events', \
    value_format ='JSON'); 
```

```sql
CREATE TABLE play_events_sum as SELECT song_id, COUNT(*) FROM s_play_events WINDOW TUMBLING (SIZE 30 SECONDS) GROUP BY song_id; 
```

2) Query `KSQL` tables

```sql
SELECT * from play_events_sum WINDOW TUMBLING (SIZE 10 SECONDS);
```

## Performance tests

Pre-requisites:

* [Vegeta](https://github.com/tsenart/vegeta)

#### Usage

1) Run tests

```
vegeta attack \
    -targets=get_top_five_songs.txt \
    -duration=30s \
    -rate=1200 \
    -timeout=60s \
    -insecure \
    > output/report_get_top_five_songs.bin
```

2) Produce report

```
vegeta report \
    -inputs=output/report_get_top_five_songs.bin \
    -reporter=plot \
    > output/plot_get_top_five_songs.html
```

or in text form:

```
vegeta report \
    -inputs=output/report_get_top_five_songs.bin \
    -reporter=text 
```

#### Kafka storage - sample results (for localhost)

```
13:50 $ vegeta report     -inputs=output/report_get_top_five_songs.bin     -reporter=text
Requests      [total, rate]            36000, 1200.03
Duration      [total, attack, wait]    29.999525297s, 29.999154592s, 370.705µs
Latencies     [mean, 50, 95, 99, max]  436.465µs, 400.87µs, 630.034µs, 860.437µs, 5.125359ms
Bytes In      [total, mean]            15444000, 429.00
Bytes Out     [total, mean]            0, 0.00
Success       [ratio]                  100.00%
Status Codes  [code:count]             200:36000
Error Set:
```

```
13:52 $ vegeta report     -inputs=output/report_get_top_five_songs.bin     -reporter=text
Requests      [total, rate]            60000, 2000.03
Duration      [total, attack, wait]    29.999866154s, 29.999499913s, 366.241µs
Latencies     [mean, 50, 95, 99, max]  350.012µs, 316.341µs, 508.236µs, 890.963µs, 6.411105ms
Bytes In      [total, mean]            25740000, 429.00
Bytes Out     [total, mean]            0, 0.00
Success       [ratio]                  100.00%
Status Codes  [code:count]             200:60000
Error Set:
```

```
13:58 $ vegeta report     -inputs=output/report_get_top_five_songs.bin     -reporter=text
Requests      [total, rate]            90000, 3000.04
Duration      [total, attack, wait]    29.999995326s, 29.99963659s, 358.736µs
Latencies     [mean, 50, 95, 99, max]  330.818µs, 288.892µs, 498.568µs, 935.044µs, 11.946227ms
Bytes In      [total, mean]            39132685, 434.81
Bytes Out     [total, mean]            0, 0.00
Success       [ratio]                  100.00%
Status Codes  [code:count]             200:90000
Error Set:
```

```
14:02 $ vegeta report     -inputs=output/report_get_top_five_songs.bin     -reporter=text
Requests      [total, rate]            180000, 6000.06
Duration      [total, attack, wait]    30.000087223s, 29.999713275s, 373.948µs
Latencies     [mean, 50, 95, 99, max]  320.237µs, 281.254µs, 467.589µs, 777.762µs, 6.130985ms
Bytes In      [total, mean]            78002859, 433.35
Bytes Out     [total, mean]            0, 0.00
Success       [ratio]                  100.00%
Status Codes  [code:count]             200:180000
Error Set:
```

```
14:03 $ vegeta report     -inputs=output/report_get_top_five_songs.bin     -reporter=text
Requests      [total, rate]            300000, 10000.03
Duration      [total, attack, wait]    30.000279111s, 29.999899858s, 379.253µs
Latencies     [mean, 50, 95, 99, max]  741.127µs, 413.179µs, 2.09382ms, 5.792056ms, 76.083959ms
Bytes In      [total, mean]            128821375, 429.40
Bytes Out     [total, mean]            0, 0.00
Success       [ratio]                  100.00%
Status Codes  [code:count]             200:300000
Error Set:
```