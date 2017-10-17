package com.mwronski.kafka;

import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.util.Arrays;

final class WebService {

    private final HostInfo hostInfo;
    private Server jettyServer;

    WebService(final HostInfo hostInfo) {
        this.hostInfo = hostInfo;
    }

    void start(Object... restControllers) throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(hostInfo.port());
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        Arrays.stream(restControllers).forEach(c -> rc.register(c));

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();
    }

    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

}

