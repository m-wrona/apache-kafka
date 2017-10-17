package com.mwronski.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("kafka-music")
public final class KafkaRestService {

    private final MetadataService metadataService;
    private final HostInfo hostInfo;
    private Server jettyServer;

    KafkaRestService(final KafkaStreams streams, final HostInfo hostInfo) {
        this.metadataService = new MetadataService(streams);
        this.hostInfo = hostInfo;
    }

    /**
     * Get the metadata for all of the instances of this Kafka Streams application
     *
     * @return List of {@link HostStoreInfo}
     */
    @GET()
    @Path("/instances")
    @Produces(MediaType.APPLICATION_JSON)
    public List<HostStoreInfo> streamsMetadata() {
        return metadataService.streamsMetadata();
    }

    /**
     * Get the metadata for all instances of this Kafka Streams application that currently
     * has the provided store.
     *
     * @param store The store to locate
     * @return List of {@link HostStoreInfo}
     */
    @GET()
    @Path("/instances/{storeName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<HostStoreInfo> streamsMetadataForStore(@PathParam("storeName") String store) {
        return metadataService.streamsMetadataForStore(store);
    }

    /**
     * Start an embedded Jetty Server
     *
     * @throws Exception
     */
    void start() throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(hostInfo.port());
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();
    }

    /**
     * Stop the Jetty Server
     *
     * @throws Exception
     */
    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

}

