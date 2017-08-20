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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import cz.net21.ttulka.thistledb.server.db.DataSourceFactory;
import lombok.extern.apachecommons.CommonsLog;

/**
 * The server.
 * <p>
 * Created by ttulka
 */
@CommonsLog
public class Server implements Runnable, AutoCloseable {

    public static final int DEFAULT_PORT = 8758;    // 9658
    public static final Path DEFAULT_DATA_DIR = Paths.get("data");

    public static final int DEFAULT_MAX_CONNECTION_POOL = 20;

    protected final int port;

    protected final DataSource dataSource;

    protected int maxClientConnections = DEFAULT_MAX_CONNECTION_POOL;

    private ServerSocketChannel serverChannel;
    private Selector selector;
    private boolean listening = false;

    private final List<ClientConnectionThread> connectionPool = new ArrayList<>();

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

    public boolean listening() {
        return listening;
    }

    public void setMaxClientConnections(int maxClientConnections) {
        if (maxClientConnections <= 0) {
            throw new ServerException("Max client connections must be greater than zero.");
        }
        this.maxClientConnections = maxClientConnections;
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

        new Thread(this).start();
    }

    /**
     * Starts the server listener and waits until it's up and running.
     *
     * @param timeout the waiting timeout in milliseconds
     */
    public void startAndWait(int timeout) {
        start();
        try {
            startLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("Waiting for the server start interrupted.", e);
        }
    }

    /**
     * Stops the server listener.
     */
    public void stop() {
        log.info("Closing a serverChannel and stopping the server...");
        listening = false;
        try {
            selector.close();
        } catch (IOException e) {
            log.warn("Cannot close a selector", e);
        }
    }

    /**
     * Stops the server listener and waits until it's down.
     *
     * @param timeout the waiting timeout in milliseconds
     */
    public void stopAndWait(int timeout) {
        stop();
        try {
            stopLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("Waiting for the server stop interrupted.", e);
        }
    }

    /**
     * Calls {@link #stop()}.
     */
    @Override
    public final void close() {
        stopAndWait(5000);
    }

    private void afterClosed() {
        log.info("**************** afterClosed"); // TODO remove
        closeServerChannel();

        dataSource.cleanUpData();
    }

    /**
     * Run method of the server listening.
     *
     * @throws ServerException when something goes wrong
     */
    @Override
    public void run() {
        log.info("Opening a serverChannel on " + port + " and waiting for client requests...");
        ExecutorService executor = Executors.newCachedThreadPool();
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
                            executor.execute(acceptNewClientConnection());
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
                e.printStackTrace();
                throw new ServerException("Exception while serving client connections.", e);
            } else {
                log.warn("Server was stopped while serving client connections.", e);
            }
        } finally {
            log.info("EXECUTOR shutdown"); // TODO remove
            shutdownClientConnections();
            executor.shutdown();
            while (!executor.isTerminated()) {
            }
            log.info("EXECUTOR is finally down"); // TODO remove
            afterClosed();

            log.info("STOP latch countdown"); // TODO remove
            stopLatch.countDown(); // server is stopped
        }
    }

    private int clientNumber = 0;

    private ClientConnectionThread acceptNewClientConnection() throws IOException {
        log.info("Accepting a new connection " + clientNumber); // TODO debug
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);

        if (!connectionPoolFree()) {
            refuseConnectionOverMaxPool(clientChannel);
            throw new IllegalStateException("Maximum client connection pool exceeded.");
        }
        ClientConnectionThread thread = new ClientConnectionThread(clientNumber++, clientChannel, dataSource);
        addToConnectionPool(thread);
        return thread;
    }

    // TODO refactor the connection pool as a new class
    private boolean connectionPoolFree() {
        int countOpenConnections = 0;

        Iterator<ClientConnectionThread> iterator = connectionPool.iterator();
        while (iterator.hasNext()) {
            ClientConnectionThread thread = iterator.next();
            if (thread.isLive()) {
                countOpenConnections++;
            } else {
                iterator.remove();
            }
        }
        return countOpenConnections < maxClientConnections;
    }

    private void addToConnectionPool(ClientConnectionThread thread) {
        connectionPool.add(thread);
    }

    private void refuseConnectionOverMaxPool(SocketChannel clientChannel) {
        try {
            SocketUtils.printlnIntoChannel("REFUSED Connection Pool exceeded", clientChannel);

        } finally {
            closeClientChannel(clientChannel);
        }
    }

    private void closeClientChannel(SocketChannel clientChannel) {
        try {
            clientChannel.socket().close();
            clientChannel.close();

        } catch (Exception ignore) {
            log.warn("Exception by closing a client channel.", ignore);
        }
    }

    private void shutdownClientConnections() {
        connectionPool.stream().forEach(ClientConnectionThread::stop);
    }

    private void closeServerChannel() {
        if (serverChannel != null) {
            try {
                serverChannel.socket().close();
                serverChannel.close();
                log.info("Server socket closed"); // TODO remove

            } catch (Exception ignore) {
                log.warn("Exception by closing a server channel.", ignore);
            }
        }
    }
}
