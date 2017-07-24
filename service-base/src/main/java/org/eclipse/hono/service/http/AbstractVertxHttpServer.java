/**
 * Copyright (c) 2016, 2017 Bosch Software Innovations GmbH.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Bosch Software Innovations GmbH - initial creation
 */

package org.eclipse.hono.service.http;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.eclipse.hono.config.ServiceConfigProperties;
import org.eclipse.hono.service.AbstractProtocolAdapterBase;
import org.eclipse.hono.util.Constants;
import org.eclipse.hono.util.JwtHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

/**
 * Abstract VertX based HTTP server.
 *
 * @param <T> The type of configuration properties used by this service.
 */
public abstract class AbstractVertxHttpServer<T extends ServiceConfigProperties> extends AbstractProtocolAdapterBase<T> {
    /**
     * The <em>application/json</em> content type.
     */
    protected static final String CONTENT_TYPE_JSON = "application/json";
    /**
     * The <em>application/json; charset=utf-8</em> content type.
     */
    protected static final String CONTENT_TYPE_JSON_UFT8 = "application/json; charset=utf-8";
    /**
     * Default file uploads directory used by Vert.x Web
     */
    protected static final String DEFAULT_UPLOADS_DIRECTORY = "/tmp";
    /**
     * The name of the cookie used to store a device's registration assertion JWT token.
     */
    protected static final String HEADER_REGISTRATION_ASSERTION = "Hono-Reg-Assertion";
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractVertxHttpServer.class);

    @Value("${spring.profiles.active:}")
    private String activeProfiles;

    private HttpServer server;
    private HttpServer insecureServer;

    /**
     * Ends a response with HTTP status code 400 (Bad Request) and an optional message.
     * <p>
     * The content type of the message will be <em>text/plain</em>.
     *
     * @param response The HTTP response to write to.
     * @param msg The message to write to the response's body (may be {@code null}).
     * @throws NullPointerException if response is {@code null}.
     */
    protected static void badRequest(final HttpServerResponse response, final String msg) {
        badRequest(response, msg, null);
    }

    /**
     * Ends a response with HTTP status code 400 (Bad Request) and an optional message.
     *
     * @param response The HTTP response to write to.
     * @param msg The message to write to the response's body (may be {@code null}).
     * @param contentType The content type of the message (if {@code null}, then <em>text/plain</em> is used}.
     * @throws NullPointerException if response is {@code null}.
     */
    protected static void badRequest(final HttpServerResponse response, final String msg, final String contentType) {
        LOG.debug("Bad request: {}", msg);
        endWithStatus(response, HTTP_BAD_REQUEST, null, msg, contentType);
    }

    /**
     * Ends a response with HTTP status code 500 (Internal Error) and an optional message.
     * <p>
     * The content type of the message will be <em>text/plain</em>.
     *
     * @param response The HTTP response to write to.
     * @param msg The message to write to the response's body (may be {@code null}).
     * @throws NullPointerException if response is {@code null}.
     */
    protected static void internalServerError(final HttpServerResponse response, final String msg) {
        LOG.debug("Internal server error: {}", msg);
        endWithStatus(response, HTTP_INTERNAL_ERROR, null, msg, null);
    }

    /**
     * Ends a response with HTTP status code 503 (Service Unavailable) and sets the <em>Retry-After</em> HTTP header
     * to a given number of seconds.
     *
     * @param response The HTTP response to write to.
     * @param retryAfterSeconds The number of seconds to set in the header.
     */
    protected static void serviceUnavailable(final HttpServerResponse response, final int retryAfterSeconds) {
        serviceUnavailable(response, retryAfterSeconds, null, null);
    }

    /**
     * Ends a response with HTTP status code 503 (Service Unavailable) and sets the <em>Retry-After</em> HTTP header
     * to a given number of seconds.
     *
     * @param response The HTTP response to write to.
     * @param retryAfterSeconds The number of seconds to set in the header.
     * @param detail The message to write to the response's body (may be {@code null}).
     * @param contentType The content type of the message (if {@code null}, then <em>text/plain</em> is used}.
     * @throws NullPointerException if response is {@code null}.
     */
    protected static void serviceUnavailable(final HttpServerResponse response, final int retryAfterSeconds,
            final String detail, final String contentType) {

        LOG.debug("Service unavailable: {}", detail);
        Map<CharSequence, CharSequence> headers = new HashMap<>(2);
        headers.put(HttpHeaders.CONTENT_TYPE, contentType != null ? contentType : "text/plain");
        headers.put(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfterSeconds));
        endWithStatus(response, HTTP_UNAVAILABLE, headers, detail, contentType);
    }

    /**
     * Ends a response with a given HTTP status code and detail message.
     *
     * @param response The HTTP response to write to.
     * @param status The status code to write to the response.
     * @param headers HTTP headers to set on the response (may be {@code null}).
     * @param detail The message to write to the response's body (may be {@code null}).
     * @param contentType The content type of the message (if {@code null}, then <em>text/plain</em> is used}.
     * @throws NullPointerException if response is {@code null}.
     */
    protected static void endWithStatus(final HttpServerResponse response, final int status,
            Map<CharSequence, CharSequence> headers, final String detail, final String contentType) {

        Objects.requireNonNull(response);
        response.setStatusCode(status);
        if (headers != null) {
            for (Map.Entry<CharSequence, CharSequence> header : headers.entrySet()) {
                response.putHeader(header.getKey(), header.getValue());
            }
        }
        if (detail != null) {
            if (contentType != null) {
                response.putHeader(HttpHeaders.CONTENT_TYPE, contentType);
            } else {
                response.putHeader(HttpHeaders.CONTENT_TYPE, "text/plain");
            }
            response.end(detail);
        } else {
            response.end();
        }
    }

    /**
     * Gets the value of the <em>Content-Type</em> HTTP header for a request.
     *
     * @param ctx The routing context containing the HTTP request.
     * @return The content type or {@code null} if the request doesn't contain a
     *         <em>Content-Type</em> header.
     * @throws NullPointerException if context is {@code null}.
     */
    protected static String getContentType(final RoutingContext ctx) {

        return Objects.requireNonNull(ctx).request().getHeader(HttpHeaders.CONTENT_TYPE);
    }

    /**
     * The default port.
     *
     * Subclasses should override this method to provide a service specific default value.
     * <p>
     * @return 8443
     */
    @Override
    public int getPortDefaultValue() {
        return 8443;
    }

    /**
     * The default insecure port.
     *
     * Subclasses should override this method to provide a service specific default value.
     * <p>
     * @return 8443
     */
    @Override
    public int getInsecurePortDefaultValue() {
        return 8080;
    }

    @Override
    protected final int getActualPort() {
        return (server != null ? server.actualPort() : Constants.PORT_UNCONFIGURED);
    }

    @Override
    protected final int getActualInsecurePort() {
        return (insecureServer != null ? insecureServer.actualPort() : Constants.PORT_UNCONFIGURED);
    }

    /**
     * Sets the http server instance configured to serve requests over a TLS secured socket.
     * <p>
     * If no server is set using this method, then a server instance is created during
     * startup of this adapter based on the <em>config</em> properties and the server options
     * returned by {@link #getHttpServerOptions()}.
     *
     * @param server The http server.
     * @throws NullPointerException if server is {@code null}.
     * @throws IllegalArgumentException if the server is already started and listening on an address/port.
     */
    @Autowired(required = false)
    public final void setHttpServer(final HttpServer server) {
        Objects.requireNonNull(server);
        if (server.actualPort() > 0) {
            throw new IllegalArgumentException("http server must not be started already");
        } else {
            this.server = server;
        }
    }

    /**
     * Sets the http server instance configured to serve requests over a plain socket.
     * <p>
     * If no server is set using this method, then a server instance is created during
     * startup of this adapter based on the <em>config</em> properties and the server options
     * returned by {@link #getInsecureHttpServerOptions()}.
     *
     * @param server The http server.
     * @throws NullPointerException if server is {@code null}.
     * @throws IllegalArgumentException if the server is already started and listening on an address/port.
     */
    @Autowired(required = false)
    public final void setInsecureHttpServer(final HttpServer server) {
        Objects.requireNonNull(server);
        if (server.actualPort() > 0) {
            throw new IllegalArgumentException("http server must not be started already");
        } else {
            this.insecureServer = server;
        }
    }

    @Override
    public final void doStart(Future<Void> startFuture) {

        checkPortConfiguration()
            .compose(s -> preStartup())
            .compose(s -> {
                Router router = createRouter();
                if (router == null) {
                    return Future.failedFuture("no router configured");
                } else {
                    addRoutes(router);
                    return CompositeFuture.all(bindSecureHttpServer(router), bindInsecureHttpServer(router));
                }
            })
            .compose(s -> {
                connectToMessaging(null);
                connectToDeviceRegistration(null);
                connectToCredentialsService(null);
                try {
                    onStartupSuccess();
                    startFuture.complete();
                } catch (Exception e) {
                    LOG.error("error in onStartupSuccess", e);
                    startFuture.fail(e);
                }
            }, startFuture);
    }

    /**
     * Invoked before the http server is started.
     * <p>
     * May be overridden by sub-classes to provide additional startup handling.
     *
     * @return A future indicating the outcome of the operation. The start up process fails if the returned future fails.
     */
    protected Future<Void> preStartup() {

        return Future.succeededFuture();
    }

    /**
     * Invoked after this adapter has started up successfully.
     * <p>
     * May be overridden by sub-classes.
     */
    protected void onStartupSuccess() {
        // empty
    }

    /**
     * Creates the router for handling requests.
     * <p>
     * This method creates a router instance with the following routes:
     * <ol>
     * <li>A default route limiting the body size of requests to the maximum payload size set in the <em>config</em> properties.</li>
     * <li>A route for retrieving this adapter's current status from the resource path returned by
     * {@link #getStatusResourcePath()} (if not {@code null}).</li>
     * </ol>
     *
     * @return The newly created router (never {@code null}).
     */
    protected Router createRouter() {

        final Router router = Router.router(vertx);
        LOG.info("limiting size of inbound request body to {} bytes", getConfig().getMaxPayloadSize());
        router.route().handler(BodyHandler.create().setBodyLimit(getConfig().getMaxPayloadSize()).setUploadsDirectory(DEFAULT_UPLOADS_DIRECTORY));

        String statusResourcePath = getStatusResourcePath();
        if (statusResourcePath != null) {
            router.route(HttpMethod.GET, statusResourcePath).handler(this::doGetStatus);
        }

        return router;
    }

    /**
     * Returns the path for the status resource.
     * <p>
     * By default, this method returns {@code /status}.
     * Subclasses may override this method to return a different path or {@code null},
     * in which case the status resource will be disabled.
     *
     * @return The resource path or {@code null}.
     */
    protected String getStatusResourcePath() {
        return "/status";
    }

    /**
     * Adds custom routes for handling requests.
     * <p>
     * This method is invoked right before the http server is started with the value returned by
     * {@link #createRouter()}.
     *
     * @param router The router to add the custom routes to.
     */
    protected abstract void addRoutes(final Router router);

    /**
     * Gets the options to use for creating the TLS secured http server.
     * <p>
     * Subclasses may override this method in order to customize the server.
     * <p>
     * This method returns default options with the host and port being set to the corresponding values
     * from the <em>config</em> properties and using a maximum chunk size of 4096 bytes.
     *
     * @return The http server options.
     */
    protected HttpServerOptions getHttpServerOptions() {

        HttpServerOptions options = new HttpServerOptions();
        options.setHost(getConfig().getBindAddress()).setPort(getConfig().getPort(getPortDefaultValue())).setMaxChunkSize(4096);
        addTlsKeyCertOptions(options);
        addTlsTrustOptions(options);
        return options;
    }

    /**
     * Gets the options to use for creating the insecure http server.
     * <p>
     * Subclasses may override this method in order to customize the server.
     * <p>
     * This method returns default options with the host and port being set to the corresponding values
     * from the <em>config</em> properties and using a maximum chunk size of 4096 bytes.
     *
     * @return The http server options.
     */
    protected HttpServerOptions getInsecureHttpServerOptions() {

        HttpServerOptions options = new HttpServerOptions();
        options.setHost(getConfig().getInsecurePortBindAddress()).setPort(getConfig().getInsecurePort(getInsecurePortDefaultValue())).setMaxChunkSize(4096);
        return options;
    }

    private Future<HttpServer> bindSecureHttpServer(final Router router) {

        if (isSecurePortEnabled()) {
            Future<HttpServer> result = Future.future();
            final String bindAddress = server == null ? getConfig().getBindAddress() : "?";
            if (server == null) {
                server = vertx.createHttpServer(getHttpServerOptions());
            }
            server.requestHandler(router::accept).listen(done -> {
                if (done.succeeded()) {
                    LOG.info("secure http server listening on {}:{}", bindAddress, server.actualPort());
                    result.complete(done.result());
                } else {
                    LOG.error("error while starting up secure http server", done.cause());
                    result.fail(done.cause());
                }
            });
            return result;
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<HttpServer> bindInsecureHttpServer(final Router router) {

        if (isInsecurePortEnabled()) {
            Future<HttpServer> result = Future.future();
            final String bindAddress = insecureServer == null ? getConfig().getInsecurePortBindAddress() : "?";
            if (insecureServer == null) {
                insecureServer = vertx.createHttpServer(getInsecureHttpServerOptions());
            }
            insecureServer.requestHandler(router::accept).listen(done -> {
                if (done.succeeded()) {
                    LOG.info("insecure http server listening on {}:{}", bindAddress, insecureServer.actualPort());
                    result.complete(done.result());
                } else {
                    LOG.error("error while starting up insecure http server", done.cause());
                    result.fail(done.cause());
                }
            });
            return result;
        } else {
            return Future.succeededFuture();
        }
    }

    @Override
    public final void doStop(Future<Void> stopFuture) {

        try {
            preShutdown();
        } catch (Exception e) {
            LOG.error("error in preShutdown", e);
        }

        Future<Void> shutdownTracker = Future.future();
        shutdownTracker.setHandler(done -> {
            if (done.succeeded()) {
                LOG.info("HTTP adapter has been shut down successfully");
                stopFuture.complete();
            } else {
                LOG.info("error while shutting down adapter", done.cause());
                stopFuture.fail(done.cause());
            }
        });

        Future<Void> serverStopTracker = Future.future();
        if (server != null) {
            server.close(serverStopTracker.completer());
        } else {
            serverStopTracker.complete();
        }

        Future<Void> insecureServerStopTracker = Future.future();
        if (insecureServer != null) {
            insecureServer.close(insecureServerStopTracker.completer());
        } else {
            insecureServerStopTracker.complete();
        }

        CompositeFuture.all(serverStopTracker, insecureServerStopTracker)
            .compose(v -> {
                Future<Void> honoClientStopTracker = Future.future();
                closeClients(honoClientStopTracker.completer());
                return honoClientStopTracker;
            }).compose(v -> postShutdown())
            .compose(s -> shutdownTracker.complete(), shutdownTracker);
    }

    /**
     * Invoked before the Http server is shut down.
     * May be overridden by sub-classes.
     */
    protected void preShutdown() {
        // empty
    }

    /**
     * Invoked after the Adapter has been shutdown successfully.
     * May be overridden by sub-classes to provide further shutdown handling.
     *
     * @return A future that has to be completed when this operation is finished.
     */
    protected Future<Void> postShutdown() {
        return Future.succeededFuture();
    }

    private void doGetStatus(final RoutingContext ctx) {
        JsonObject result = new JsonObject(getHonoMessagingClient().getConnectionStatus());
        result.put("active profiles", activeProfiles);
        result.put("senders", getHonoMessagingClient().getSenderStatus());
        adaptStatusResource(result);
        ctx.response()
            .putHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_JSON)
            .end(result.encodePrettily());
    }

    /**
     * Adapts the JsonObject returned on a status request.
     * Subclasses can add their own properties here.
     *
     * @param status status object to be adapted
     */
    protected void adaptStatusResource(final JsonObject status) {
        // empty
    }

    /**
     * Gets a registration assertion for a device.
     * <p>
     * This method first tries to retrieve the assertion from request header {@link #HEADER_REGISTRATION_ASSERTION}.
     * If the header exists and contains a value representing a non-expired assertion, a completed future
     * containing the header field's value is returned.
     * Otherwise a new assertion is retrieved from the Device Registration service and included in the response
     * using the same header name.
     *
     * @param ctx The routing context to use for getting/setting the cookie.
     * @param tenantId The tenant that the device belongs to.
     * @param deviceId The device to get the assertion for.
     * @return A future containing the assertion.
     */
    protected final Future<String> getRegistrationAssertionHeader(final RoutingContext ctx, final String tenantId,
            final String deviceId) {

        String assertion = ctx.request().getHeader(HEADER_REGISTRATION_ASSERTION);
        if (assertion != null && !JwtHelper.isExpired(assertion, 5)) {
            return Future.succeededFuture(assertion);
        } else {
            return getRegistrationAssertion(tenantId, deviceId).compose(token -> {
                ctx.response().putHeader(HEADER_REGISTRATION_ASSERTION, token);
                return Future.succeededFuture(token);
            });
        }
    }
}