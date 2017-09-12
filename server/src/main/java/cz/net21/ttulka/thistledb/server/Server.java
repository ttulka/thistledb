package cz.net21.ttulka.thistledb.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import cz.net21.ttulka.thistledb.db.DataSource;
import lombok.extern.apachecommons.CommonsLog;

/**
 * The server.
 *
 * @author ttulka
 */
@CommonsLog
public class Server implements AutoCloseable {

    public static final int DEFAULT_PORT = 9658;
    public static final Path DEFAULT_DATA_DIR = Paths.get("data");

    public static final int DEFAULT_MAX_CONNECTION_POOL = 20;

    protected final int port;

    protected final DataSource dataSource;

    protected int maxClientConnections = DEFAULT_MAX_CONNECTION_POOL;

    private Selector selector;

    private boolean listening = false;

    private ServerConnectionPool connectionPool = new ServerConnectionPool();

    private CountDownLatch startLatch;
    private CountDownLatch stopLatch;

    public Server() {
        this(DEFAULT_PORT);
    }

    public Server(int port) {
        this(port, DEFAULT_DATA_DIR);
    }

    public Server(Path dataDir) {
        this(DEFAULT_PORT, dataDir);
    }

    public Server(int port, Path dataDir) {
        this.port = port;
        this.dataSource = DataSourceFactory.getDataSource(dataDir);
    }

    public int getPort() {
        return port;
    }

    public int getMaxClientConnections() {
        return maxClientConnections;
    }

    public void setMaxClientConnections(int maxClientConnections) {
        if (maxClientConnections <= 0) {
            throw new ServerException("Max client connections must be greater than zero.");
        }
        this.maxClientConnections = maxClientConnections;
    }

    /**
     * Is the server listening (running).
     *
     * @return true if the server is listening, otherwise false
     */
    public boolean listening() {
        return listening;
    }

    /**
     * Starts the server listener.
     */
    public final void start() {
        if (listening) {
            throw new IllegalStateException("Server already started.");
        }
        listening = true;

        startLatch = new CountDownLatch(1);
        stopLatch = new CountDownLatch(1);

        new Thread(this::run).start();
    }

    /**
     * Starts the server listener and waits until it's up and running. Blocking variant.
     *
     * @param timeout the waiting timeout in milliseconds
     */
    public void start(int timeout) {
        start();
        try {
            startLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("Waiting for the server start interrupted.", e);
        }
    }

    /**
     * Stops the server listener and blocks until it's down.
     *
     * @param timeout the waiting timeout in milliseconds
     */
    public void stop(int timeout) {
        stopServer();
        try {
            stopLatch.await(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Waiting for the server stop interrupted.", e);
        }
    }

    /**
     * Stops the server listener and blocks until it's down.
     */
    public void stop() {
        stopServer();
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.warn("Waiting for the server stop interrupted.", e);
        }
    }

    private void stopServer() {
        log.info("Closing a serverChannel and stopping the server...");
        listening = false;
        selector.wakeup();
    }

    /**
     * Calls {@link #stop()}.
     */
    @Override
    public final void close() {
        stop();
    }

    /**
     * Run method of the server listening.
     *
     * @throws ServerException when something goes wrong
     */
    protected void run() {
        log.info("Opening a serverChannel on " + port + " and waiting for client requests...");

        ExecutorService executor = Executors.newCachedThreadPool();

        ServerSocketChannel serverChannel = null;
        try {
            selector = Selector.open();

            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(new InetSocketAddress(port));
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            startLatch.countDown(); // server is started

            while (listening) {
                selector.select();

                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove(); // prevent the same key from coming up again the next time around

                    try {
                        if (!key.isValid()) {
                            continue;
                        }

                        if (key.isAcceptable()) {
                            ClientConnectionThread clientConnectionThread = acceptNewClientConnection(serverChannel);
                            executor.execute(clientConnectionThread);
                        }
                    } catch (Exception e) {
                        log.error("Error by accepting a client connection.", e);
                        key.cancel();
                    }
                }
            }
        } catch (ClosedSelectorException ignore) {
            // this happens when selector is waiting and server was stopped
        } catch (Exception e) {
            if (listening) {
                listening = false;
                throw new ServerException("Exception while serving client connections.", e);
            } else {
                log.warn("Server was stopped while serving client connections.", e);
            }
        } finally {
            connectionPool.shutdownClientConnections();
            executor.shutdown();
            while (!executor.isTerminated()) {
            }

            closeSelector(selector);
            closeServerChannel(serverChannel);
            afterServerClosed();

            stopLatch.countDown(); // server is stopped
        }
    }

    private void closeSelector(Selector selector) {
        if (selector != null) {
            try {
                selector.close();

            } catch (IOException e) {
                log.warn("Cannot close a selector", e);
            }
        }
    }

    private void closeServerChannel(ServerSocketChannel serverChannel) {
        if (serverChannel != null) {
            try {
                serverChannel.close();

            } catch (Exception ignore) {
                log.warn("Exception by closing a server channel.", ignore);
            }
        }
    }

    /**
     * Tidy up after the server was closed.
     */
    protected void afterServerClosed() {
        dataSource.cleanUpData();
    }

    private ClientConnectionThread acceptNewClientConnection(ServerSocketChannel serverChannel) throws IOException {
        log.debug("Accepting a new connection.");

        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);

        if (!connectionPool.isConnectionPoolFree(maxClientConnections)) {
            refuseConnectionOverMaxPool(clientChannel);
            throw new IllegalStateException("Maximum client connection pool exceeded.");
        }
        ClientConnectionThread thread = new ClientConnectionThread(clientChannel, dataSource);
        connectionPool.addClientConnection(thread);
        return thread;
    }

    private void refuseConnectionOverMaxPool(SocketChannel clientChannel) {
        SocketUtils.printlnIntoChannel("REFUSED Connection Pool exceeded", clientChannel);
    }
}