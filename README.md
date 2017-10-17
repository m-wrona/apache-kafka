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

```
docker-compose up
```

and then:

```
./run_web.sh
```