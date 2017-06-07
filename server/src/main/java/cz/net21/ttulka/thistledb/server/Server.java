package cz.net21.ttulka.thistledb.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final int port;
    private final DataSource dataSource;

    private ServerSocket socket;
    private AtomicBoolean listening = new AtomicBoolean(false);

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

    @Override
    public void run() {
        if (listening.compareAndSet(false, true)) {
            ExecutorService executor = Executors.newCachedThreadPool();

            log.info("Opening a socket on " + port + " and waiting for client requests...");
            try {
                socket = new ServerSocket(port);

                startLatch.countDown();

                while (listening()) {
                    Socket clientSocket = socket.accept();

                    ServerThread serverThread = new ServerThread(clientSocket, dataSource, this::listening);
                    executor.execute(serverThread);
                }
                executor.shutdown();
                while (!executor.isTerminated()) {
                }
            } catch (Exception e) {
                if (listening()) {
                    throw new RuntimeException("Cannot listen on port " + port + ".", e);
                }
            } finally {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        log.warn("Cannot close a socket", e);
                    }
                }
            }
        }
    }

    /**
     * Stops the server listener.
     */
    public void stop() {
        log.info("Closing a socket and stopping the server...");

        listening.set(false);

        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                log.warn("Cannot close a socket", e);
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
