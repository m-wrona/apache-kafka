package com.mwronski.kafka;

import com.mwronski.kafka.music.ChartsStream;
import io.confluent.examples.streams.avro.PlayEvent;
import io.confluent.examples.streams.avro.Song;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.*;

public class KafkaDataGenerator {

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : Application.DEFAULT_BOOTSTRAP_SERVERS;
        final String schemaRegistryUrl = args.length > 1 ? args[1] : Application.DEFAULT_SCHEMA_REGISTRY_URL;

        System.out.println("***************************************************************************");
        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);
        System.out.println("***************************************************************************");

        final List<Song> songs = generateSongs();
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final SpecificAvroSerializer<PlayEvent> playEventSerializer = new SpecificAvroSerializer<>();
        playEventSerializer.configure(serdeConfig, false);
        final SpecificAvroSerializer<Song> songSerializer = new SpecificAvroSerializer<>();
        songSerializer.configure(serdeConfig, false);

        final KafkaProducer<String, PlayEvent> playEventProducer = new KafkaProducer<>(props,
                Serdes.String().serializer(),
                playEventSerializer);

        final KafkaProducer<Long, Song> songProducer = new KafkaProducer<>(props,
                new LongSerializer(),
                songSerializer);

        songs.forEach(song -> {
            System.out.println("Writing song information for '" + song.getName() + "' to input topic " +
                    ChartsStream.SONG_FEED);
            songProducer.send(new ProducerRecord<>(ChartsStream.SONG_FEED, song.getId(), song));
        });

        songProducer.close();
        final long duration = 60 * 1000L;
        final Random random = new Random();

        // send a play event every 100 milliseconds
        while (true) {
            final Song song = songs.get(random.nextInt(songs.size()));
            System.out.println("Writing play event for song " + song.getName() + " to input topic " +
                    ChartsStream.PLAY_EVENTS);
            playEventProducer.send(
                    new ProducerRecord<>(ChartsStream.PLAY_EVENTS,
                            "uk", new PlayEvent(song.getId(), duration)));
            Thread.sleep(100L);
        }
    }

    public static List<Song> generateSongs() {
        return Arrays.asList(
                new Song(1L,
                        "Fresh Fruit For Rotting Vegetables",
                        "Dead Kennedys",
                        "Chemical Warfare",
                        "Punk"),
                new Song(2L,
                        "We Are the League",
                        "Anti-Nowhere League",
                        "Animal",
                        "Punk"),
                new Song(3L,
                        "Live In A Dive",
                        "Subhumans",
                        "All Gone Dead",
                        "Punk"),
                new Song(4L,
                        "PSI",
                        "Wheres The Pope?",
                        "Fear Of God",
                        "Punk"),
                new Song(5L,
                        "Totally Exploited",
                        "The Exploited",
                        "Punks Not Dead",
                        "Punk"),
                new Song(6L,
                        "The Audacity Of Hype",
                        "Jello Biafra And The Guantanamo School Of "
                                + "Medicine",
                        "Three Strikes",
                        "Punk"),
                new Song(7L,
                        "Licensed to Ill",
                        "The Beastie Boys",
                        "Fight For Your Right",
                        "Hip Hop"),
                new Song(8L,
                        "De La Soul Is Dead",
                        "De La Soul",
                        "Oodles Of O's",
                        "Hip Hop"),
                new Song(9L,
                        "Straight Outta Compton",
                        "N.W.A",
                        "Gangsta Gangsta",
                        "Hip Hop"),
                new Song(10L,
                        "Fear Of A Black Planet",
                        "Public Enemy",
                        "911 Is A Joke",
                        "Hip Hop"),
                new Song(11L,
                        "Curtain Call - The Hits",
                        "Eminem",
                        "Fack",
                        "Hip Hop"),
                new Song(12L,
                        "The Calling",
                        "Hilltop Hoods",
                        "The Calling",
                        "Hip Hop")

        );
    }

}
