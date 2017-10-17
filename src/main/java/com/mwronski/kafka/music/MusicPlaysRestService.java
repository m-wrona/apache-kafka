package com.mwronski.kafka.music;

import com.mwronski.kafka.Application;
import com.mwronski.kafka.music.model.SongBean;
import com.mwronski.kafka.music.model.SongPlayCountBean;
import com.mwronski.kafka.music.model.TopFiveSongs;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.glassfish.jersey.jackson.JacksonFeature;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import com.mwronski.kafka.music.model.avro.Song;
import com.mwronski.kafka.config.HostStoreInfo;
import com.mwronski.kafka.config.MetadataService;

/**
 * A simple REST proxy that runs embedded in the {@link Application}. This is used to
 * demonstrate how a developer can use the Interactive Queries APIs exposed by Kafka Streams to
 * locate and query the State Stores within a Kafka Streams Application.
 */
@Path("music")
public final class MusicPlaysRestService {

    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private LongSerializer serializer = new LongSerializer();

    public MusicPlaysRestService(final KafkaStreams streams, final HostInfo hostInfo) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
        this.hostInfo = hostInfo;
    }

    @GET
    @Path("/charts/genre/{genre}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<SongPlayCountBean> genreCharts(@PathParam("genre") final String genre) {

        // The genre might be hosted on another instance. We need to find which instance it is on
        // and then perform a remote lookup if necessary.
        final HostStoreInfo host = metadataService.streamsMetadataForStoreAndKey(Application.TOP_FIVE_SONGS_BY_GENRE_STORE, genre, new StringSerializer());

        // genre is on another instance. call the other instance to fetch the data.
        if (!thisHost(host)) {
            return fetchSongPlayCount(host, "kafka-music/charts/genre/" + genre);
        }

        // genre is on this instance
        return topFiveSongs(genre.toLowerCase(), Application.TOP_FIVE_SONGS_BY_GENRE_STORE);

    }

    @GET
    @Path("/charts/top-five")
    @Produces(MediaType.APPLICATION_JSON)
    public List<SongPlayCountBean> topFive() {
        // The top-five might be hosted elsewhere. There is only one 1 partition with data
        // so we need to first find where it is and then we can do a local or remote lookup.
        final HostStoreInfo
                host =
                metadataService.streamsMetadataForStoreAndKey(Application.TOP_FIVE_SONGS_STORE, Application
                        .TOP_FIVE_KEY, new StringSerializer());

        // top-five is hosted on another instance
        if (!thisHost(host)) {
            return fetchSongPlayCount(host, "kafka-music/charts/top-five/");
        }

        // top-five is hosted locally. so lookup in local store
        return topFiveSongs(Application.TOP_FIVE_KEY, Application.TOP_FIVE_SONGS_STORE);
    }

    private boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }


    private List<SongPlayCountBean> fetchSongPlayCount(final HostStoreInfo host, final String path) {
        return client.target(String.format("http://%s:%d/%s", host.getHost(), host.getPort(), path))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(new GenericType<List<SongPlayCountBean>>() {
                });
    }

    private List<SongPlayCountBean> topFiveSongs(final String key,
                                                 final String storeName) {

        final ReadOnlyKeyValueStore<String, TopFiveSongs> topFiveStore =
                streams.store(storeName, QueryableStoreTypes.<String, TopFiveSongs>keyValueStore());
        // Get the value from the store
        final TopFiveSongs value = topFiveStore.get(key);
        if (value == null) {
            throw new NotFoundException(String.format("Unable to find value in %s for key %s", storeName, key));
        }
        final List<SongPlayCountBean> results = new ArrayList<>();
        value.forEach(songPlayCount -> {
            final HostStoreInfo host = metadataService.streamsMetadataForStoreAndKey(Application.ALL_SONGS, songPlayCount.getSongId(), serializer);

            // if the song is not hosted on this instance then we need to lookup it up
            // on the instance it is on.
            if (!thisHost(host)) {
                final SongBean song =
                        client.target(String.format("http://%s:%d/kafka-music/song/%d",
                                host.getHost(),
                                host.getPort(),
                                songPlayCount.getSongId()))
                                .request(MediaType.APPLICATION_JSON_TYPE)
                                .get(SongBean.class);
                results.add(new SongPlayCountBean(song.getArtist(), song.getAlbum(), song.getName(),
                        songPlayCount.getPlays()));
            } else {
                // look in the local store
                final ReadOnlyKeyValueStore<Long, Song> songStore = streams.store(Application.ALL_SONGS,
                        QueryableStoreTypes.<Long, Song>keyValueStore());
                final Song song = songStore.get(songPlayCount.getSongId());
                results.add(new SongPlayCountBean(song.getArtist(), song.getAlbum(), song.getName(),
                        songPlayCount.getPlays()));
            }


        });
        return results;
    }

    @GET()
    @Path("/song/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public SongBean song(@PathParam("id") Long songId) {
        final ReadOnlyKeyValueStore<Long, Song> songStore = streams.store(Application.ALL_SONGS, QueryableStoreTypes.<Long, Song>keyValueStore());
        final Song song = songStore.get(songId);
        if (song == null) {
            throw new NotFoundException(String.format("Song with id [%d] was not found", songId));
        }
        return new SongBean(song.getArtist(), song.getAlbum(), song.getName());
    }

}

