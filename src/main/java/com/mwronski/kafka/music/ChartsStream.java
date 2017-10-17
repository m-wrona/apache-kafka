package com.mwronski.kafka.music;

import com.mwronski.kafka.config.KafkaConfigUtils;
import com.mwronski.kafka.music.model.TopFiveSongs;
import com.mwronski.kafka.music.model.avro.TopFiveSerde;
import io.confluent.examples.streams.avro.PlayEvent;
import io.confluent.examples.streams.avro.Song;
import io.confluent.examples.streams.avro.SongPlayCount;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public final class ChartsStream {

    private static final Long MIN_CHARTABLE_DURATION = 30 * 1000L;
    private static final String SONG_PLAY_COUNT_STORE = "song-play-count";
    public static final String PLAY_EVENTS = "play-events";
    public static final String SONG_FEED = "song-feed";
    public static final String ALL_SONGS = "all-songs";
    public static final String TOP_FIVE_SONGS_BY_GENRE_STORE = "top-five-songs-by-genre";
    public static final String TOP_FIVE_SONGS_STORE = "top-five-songs";
    public static final String TOP_FIVE_KEY = "all";

    public static KafkaStreams create(String bootstrapServers, String schemaRegistryUrl, int restEndpointPort) {
        final KafkaStreams streams = ChartsStream.create(
                bootstrapServers,
                schemaRegistryUrl,
                restEndpointPort,
                "/tmp/kafka-streams"
        );

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
        return streams;
    }

    public static KafkaStreams create(final String bootstrapServers, final String schemaRegistryUrl, final int applicationServerPort, final String stateDir) {
        final Properties streamsConfiguration = KafkaConfigUtils.createStreamConfig(bootstrapServers, applicationServerPort, stateDir);

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
        final KStream<String, PlayEvent> playEvents = builder.stream(Serdes.String(), playEventSerde, PLAY_EVENTS);

        // get table and create a state store to hold all the songs in the store
        final KTable<Long, Song> songTable = builder.table(Serdes.Long(), valueSongSerde, SONG_FEED, ALL_SONGS);

        // Accept play events that have a duration >= the minimum
        final KStream<Long, PlayEvent> playsBySongId = playEvents
                .filter((region, event) -> event.getDuration() >= MIN_CHARTABLE_DURATION)
                .map((key, playEvent) -> KeyValue.pair(playEvent.getSongId(), playEvent));

        // join the plays with song as we will use it later for charting
        final KStream<Long, Song> songPlays = playsBySongId.leftJoin(
                songTable,
                (playEvent, song) -> song,
                Serdes.Long(),
                playEventSerde
        );

        // create a state store to track song play counts
        final KTable<Song, Long> songPlayCounts = songPlays
                .groupBy((songId, song) -> song, keySongSerde, valueSongSerde)
                .count(SONG_PLAY_COUNT_STORE);

        final TopFiveSerde topFiveSerde = new TopFiveSerde();

        // Compute the top five charts for each genre. The results of this computation will continuously update the state
        // store "top-five-songs-by-genre", and this state store can then be queried interactively via a REST API (cf.
        // MusicPlaysRestService) for the latest charts per genre.
        songPlayCounts
                .groupBy(
                        (song, plays) -> KeyValue.pair(song.getGenre().toLowerCase(), new SongPlayCount(song.getId(), plays)),
                        Serdes.String(),
                        songPlayCountSerde
                )
                // aggregate into a TopFiveSongs instance that will keep track
                // of the current top five for each genre. The data will be available in the
                // top-five-songs-genre store
                .aggregate(
                        TopFiveSongs::new,
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
        songPlayCounts.groupBy(
                (song, plays) -> KeyValue.pair(TOP_FIVE_KEY, new SongPlayCount(song.getId(), plays)),
                Serdes.String(),
                songPlayCountSerde
        )
                .aggregate(
                        TopFiveSongs::new,
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

    private ChartsStream() {
        //no instances
    }

}
