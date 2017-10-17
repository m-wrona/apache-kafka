package com.mwronski.kafka;

import com.mwronski.kafka.config.KafkaConfigRestService;
import com.mwronski.kafka.music.ChartsStream;
import com.mwronski.kafka.music.MusicPlaysRestService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;

public final class Application {

    private static final String DEFAULT_REST_ENDPOINT_HOSTNAME = "localhost";
    static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

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


        // Start the Restful proxy for servicing remote access to state stores
        final KafkaStreams streams = ChartsStream.create(bootstrapServers, schemaRegistryUrl, restEndpointPort);
        final WebService webService = startRestProxy(streams, restEndpoint);
        addShutdownHook(streams, webService);
    }

    static void addShutdownHook(KafkaStreams streams, WebService webService) {
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                webService.stop();
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


}
