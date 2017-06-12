package cz.net21.ttulka.thistledb.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import cz.net21.ttulka.thistledb.server.db.DataSourceImpl;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ttulka
 * <p>
 * The server.
 */
@CommonsLog
public class Server implements Runnable, AutoCloseable {

    public static final int DEFAULT_PORT = 9658;
    public static final Path DEFAULT_DATA_DIR = Paths.get("data");

    public static final int DEFAULT_MAX_CONNECTION_POOL = 20;
    public static final int DEFAULT_MAX_TIMEOUT = 2 * 1000;     // 2 s

    private final int port;
    private final DataSource dataSource;

    private int maxConnectionPoolThreads = DEFAULT_MAX_CONNECTION_POOL;
    private int maxClientTimeout = DEFAULT_MAX_TIMEOUT;

    private ServerSocket serverSocket;
    private AtomicBoolean listening = new AtomicBoolean(false);

    private final AtomicInteger connectionPool = new AtomicInteger(0);

    private CountDownLatch startLatch;

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
        this.dataSource = new DataSourceImpl(dataDir);
    }

    public int getPort() {
        return port;
    }

    public int getMaxConnectionPoolThreads() {
        return maxConnectionPoolThreads;
    }

    public void setMaxConnectionPoolThreads(int maxConnectionPoolThreads) {
        this.maxConnectionPoolThreads = maxConnectionPoolThreads;
    }

    public int getMaxClientTimeout() {
        return maxClientTimeout;
    }

    public void setMaxClientTimeout(int maxClientTimeout) {
        this.maxClientTimeout = maxClientTimeout;
    }

    /**
     * Starts the server listener.
     */
    public final void start() {
        listening.set(false);
        startLatch = new CountDownLatch(1);

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
     * Run method of the server listening.
     *
     * @throws ServerException when something goes wrong
     */
    @Override
    public void run() {
        if (listening.compareAndSet(false, true)) {
            ExecutorService executor = Executors.newCachedThreadPool();

            log.info("Opening a serverSocket on " + port + " and waiting for client requests...");
            try {
                serverSocket = new ServerSocket(port);

                startLatch.countDown(); // server is started

                while (listening()) {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setSoTimeout(maxClientTimeout);

                    if (checkConnectionPoolMaxThreads(clientSocket)) {
                        connectionPool.getAndIncrement();

                        ServerThread serverThread = new ServerThread(clientSocket, dataSource, this::listening, connectionPool::decrementAndGet);
                        executor.execute(serverThread);
                    }
                }
                executor.shutdown();
                while (!executor.isTerminated()) {
                }
            } catch (Exception e) {
                if (listening()) {
                    throw new ServerException("Cannot listen on port " + port + ".", e);
                }
            } finally {
                if (serverSocket != null) {
                    try {
                        serverSocket.close();
                    } catch (IOException e) {
                        log.warn("Cannot close a serverSocket", e);
                    }
                }
            }
        }
    }

    private boolean checkConnectionPoolMaxThreads(Socket clientSocket) {
        if (connectionPool.get() >= maxConnectionPoolThreads) {

            try (PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
                out.println("REFUSED Connection Pool exceeded");

            } catch (Throwable t) {
                // ignore
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            return false;
        }
        return true;
    }

    /**
     * Stops the server listener.
     */
    public void stop() {
        log.info("Closing a serverSocket and stopping the server...");

        listening.set(false);

        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                log.warn("Cannot close a serverSocket", e);
            }
        }
    }

    public boolean listening() {
        return listening.get();
    }

    /**
     * Calls {@link #stop()}.
     */
    @Override
    public final void close() {
        stop();
    }
}
