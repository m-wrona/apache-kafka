package com.mwronski.kafka;

import com.mwronski.kafka.config.KafkaConfigRestService;
import com.mwronski.kafka.music.MusicPlaysRestService;
import com.mwronski.kafka.music.model.avro.TopFiveSerde;
import com.mwronski.kafka.music.model.TopFiveSongs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import com.mwronski.kafka.music.model.avro.PlayEvent;
import com.mwronski.kafka.music.model.avro.Song;
import com.mwronski.kafka.music.model.avro.SongPlayCount;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public final class Application {

    private static final Long MIN_CHARTABLE_DURATION = 30 * 1000L;
    private static final String SONG_PLAY_COUNT_STORE = "song-play-count";
    public static final String PLAY_EVENTS = "play-events";
    public static final String ALL_SONGS = "all-songs";
    public static final String SONG_FEED = "song-feed";
    public static final String TOP_FIVE_SONGS_BY_GENRE_STORE = "top-five-songs-by-genre";
    public static final String TOP_FIVE_SONGS_STORE = "top-five-songs";
    public static final String TOP_FIVE_KEY = "all";

    private static final String DEFAULT_REST_ENDPOINT_HOSTNAME = "localhost";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main(String[] args) throws Exception {
        if (args.length > 4) {
            throw new IllegalArgumentException("usage: ... <portForRestEndpoint> " +
                    "[<bootstrap.servers> (optional, default: " + DEFAULT_BOOTSTRAP_SERVERS + ")] " +
                    "[<schema.registry.url> (optional, default: " + DEFAULT_SCHEMA_REGISTRY_URL + ")] " +
                    "[<hostnameForRestEndPoint> (optional, default: " + DEFAULT_REST_ENDPOINT_HOSTNAME + ")]");
        }
        final int restEndpointPort = args.length == 1 ? Integer.valueOf(args[0]) : 8080;
        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";
        final String restEndpointHostname = args.length > 3 ? args[3] : DEFAULT_REST_ENDPOINT_HOSTNAME;
        final HostInfo restEndpoint = new HostInfo(restEndpointHostname, restEndpointPort);

        System.out.println("***************************************************************************");
        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);
        System.out.println("REST endpoint at http://" + restEndpointHostname + ":" + restEndpointPort);
        System.out.println("***************************************************************************");

        final KafkaStreams streams = createChartsStreams(bootstrapServers,
                schemaRegistryUrl,
                restEndpointPort,
                "/tmp/kafka-streams");

        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();

        // Now that we have finished the definition of the processing topology we can actually run
        // it via `start()`.  The Streams application as a whole can be launched just like any
        // normal Java application that has a `main()` method.
        streams.start();

        // Start the Restful proxy for servicing remote access to state stores
        final WebService restService = startRestProxy(streams, restEndpoint);

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                restService.stop();
                streams.close();
            } catch (Exception e) {
                // ignored
            }
        }));
    }

    static WebService startRestProxy(final KafkaStreams streams, final HostInfo hostInfo)
            throws Exception {
        final WebService webService = new WebService(hostInfo);
        webService.start(
                new KafkaConfigRestService(streams),
                new MusicPlaysRestService(streams, hostInfo)
        );
        return webService;
    }

    static KafkaStreams createChartsStreams(final String bootstrapServers,
                                            final String schemaRegistryUrl,
                                            final int applicationServerPort,
                                            final String stateDir) {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-music-charts");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Provide the details of our embedded http service that we'll use to connect to this streams
        // instance and discover locations of stores.
        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + applicationServerPort);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        // Set to earliest so we don't miss any data that arrived in the topics before the process
        // started
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Set the commit interval to 500ms so that any changes are flushed frequently and the top five
        // charts are updated with low latency.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        // Allow the user to fine-tune the `metadata.max.age.ms` via Java system properties from the CLI.
        // Lowering this parameter from its default of 5 minutes to a few seconds is helpful in
        // situations where the input topic was not pre-created before running the application because
        // the application will discover a newly created topic faster.  In production, you would
        // typically not change this parameter from its default.
        String metadataMaxAgeMs = System.getProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG);
        if (metadataMaxAgeMs != null) {
            try {
                int value = Integer.parseInt(metadataMaxAgeMs);
                streamsConfiguration.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, value);
                System.out.println("Set consumer configuration " + ConsumerConfig.METADATA_MAX_AGE_CONFIG +
                        " to " + value);
            } catch (NumberFormatException ignored) {
            }
        }

        // create and configure the SpecificAvroSerdes required in this example
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
        playEventSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Song> keySongSerde = new SpecificAvroSerde<>();
        keySongSerde.configure(serdeConfig, true);

        final SpecificAvroSerde<Song> valueSongSerde = new SpecificAvroSerde<>();
        valueSongSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<SongPlayCount> songPlayCountSerde = new SpecificAvroSerde<>();
        songPlayCountSerde.configure(serdeConfig, false);

        final KStreamBuilder builder = new KStreamBuilder();

        // get a stream of play events
        final KStream<String, PlayEvent> playEvents = builder.stream(Serdes.String(),
                playEventSerde,
                PLAY_EVENTS);

        // get table and create a state store to hold all the songs in the store
        final KTable<Long, Song>
                songTable =
                builder.table(Serdes.Long(), valueSongSerde, SONG_FEED, ALL_SONGS);

        // Accept play events that have a duration >= the minimum
        final KStream<Long, PlayEvent> playsBySongId =
                playEvents.filter((region, event) -> event.getDuration() >= MIN_CHARTABLE_DURATION)
                        // repartition based on song id
                        .map((key, value) -> KeyValue.pair(value.getSongId(), value));


        // join the plays with song as we will use it later for charting
        final KStream<Long, Song> songPlays = playsBySongId.leftJoin(songTable,
                (value1, song) -> song,
                Serdes.Long(),
                playEventSerde);

        // create a state store to track song play counts
        final KTable<Song, Long> songPlayCounts = songPlays.groupBy((songId, song) -> song, keySongSerde, valueSongSerde)
                .count(SONG_PLAY_COUNT_STORE);

        final TopFiveSerde topFiveSerde = new TopFiveSerde();


        // Compute the top five charts for each genre. The results of this computation will continuously update the state
        // store "top-five-songs-by-genre", and this state store can then be queried interactively via a REST API (cf.
        // MusicPlaysRestService) for the latest charts per genre.
        songPlayCounts.groupBy((song, plays) ->
                        KeyValue.pair(song.getGenre().toLowerCase(),
                                new SongPlayCount(song.getId(), plays)),
                Serdes.String(),
                songPlayCountSerde)
                // aggregate into a TopFiveSongs instance that will keep track
                // of the current top five for each genre. The data will be available in the
                // top-five-songs-genre store
                .aggregate(TopFiveSongs::new,
                        (aggKey, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        (aggKey, value, aggregate) -> {
                            aggregate.remove(value);
                            return aggregate;
                        },
                        topFiveSerde,
                        TOP_FIVE_SONGS_BY_GENRE_STORE
                );

        // Compute the top five chart. The results of this computation will continuously update the state
        // store "top-five-songs", and this state store can then be queried interactively via a REST API (cf.
        // MusicPlaysRestService) for the latest charts per genre.
        songPlayCounts.groupBy((song, plays) ->
                        KeyValue.pair(TOP_FIVE_KEY,
                                new SongPlayCount(song.getId(), plays)),
                Serdes.String(),
                songPlayCountSerde)
                .aggregate(TopFiveSongs::new,
                        (aggKey, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        (aggKey, value, aggregate) -> {
                            aggregate.remove(value);
                            return aggregate;
                        },
                        topFiveSerde,
                        TOP_FIVE_SONGS_STORE
                );

        return new KafkaStreams(builder, streamsConfiguration);

    }


}
