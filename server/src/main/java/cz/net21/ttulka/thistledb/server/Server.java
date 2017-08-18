package cz.net21.ttulka.thistledb.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import cz.net21.ttulka.thistledb.server.db.DataSourceFile;
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

    protected final int port;

    protected final DataSource dataSource;
    protected final QueryProcessor queryProcessor;

    protected int maxClientConnections = DEFAULT_MAX_CONNECTION_POOL;

    private ServerSocketChannel serverChannel;
    private Selector selector;
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
        this.dataSource = createDataSource(dataDir);
        this.queryProcessor = createQueryProcessor();
    }

    protected DataSource createDataSource(Path dataDir) {
        return new DataSourceFile(dataDir);
    }

    protected QueryProcessor createQueryProcessor() {
        return new QueryProcessor(dataSource);
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

            log.info("Opening a serverChannel on " + port + " and waiting for client requests...");
            try {
                selector = Selector.open();

                serverChannel = ServerSocketChannel.open();
                serverChannel.configureBlocking(false);
                serverChannel.socket().bind(new InetSocketAddress(port));
                serverChannel.register(selector, SelectionKey.OP_ACCEPT);

                startLatch.countDown(); // server is started

                while (listening.get()) {
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
                                acceptNewClientConnection();

                            } else if (key.isReadable()) {
                                bufferFromClientSocket(key);

                                String input;
                                while ((input = readLineFromBuffer()) != null) {
                                    processClientInput(input, (SocketChannel) key.channel());
                                }
                            }
                        } catch (Exception e) {
                            log.error("Error by serving a client.", e);

                            closeClientChannel(key);
                        }
                    }
                }
            } catch (Exception e) {
                if (listening.getAndSet(false)) {
                    throw new ServerException("Cannot listen on port " + port + ".", e);
                }
            } finally {
                if (serverChannel != null) {
                    try {
                        serverChannel.close();
                    } catch (IOException e) {
                        log.warn("Cannot close a serverChannel", e);
                    }
                }
            }
        } else {
            throw new ServerException("Server already closed.");
        }
    }

    void processClientInput(String input, SocketChannel clientChannel) {
        System.out.println("PROCESSING " + input);
        queryProcessor.process(input, output -> writeToClientSocket(output, clientChannel));
    }

    private void writeToClientSocket(String output, SocketChannel channel) {
        if (output != null) {
            System.out.println("WRITING " + output);
            ByteBuffer buffer = ByteBuffer.wrap((output + "\n").getBytes());
            while (buffer.hasRemaining()) {
                try {
                    channel.write(buffer);

                } catch (IOException e) {
                    throw new ServerException("Cannot write to a client socket.", e);
                }
            }
        }
    }

    private void acceptNewClientConnection() throws IOException {
        System.out.println("acceptNewClientConnection");
        SocketChannel channel = serverChannel.accept();
        channel.configureBlocking(false);

        if (connectionPool.get() >= maxClientConnections) {
            refuseConnectionOverMaxPool(channel.socket());
            return;
        }
        connectionPool.getAndIncrement();
        channel.register(this.selector, SelectionKey.OP_READ);
    }

    private final StringBuilder clientInputBuffer = new StringBuilder();

    private void bufferFromClientSocket(SelectionKey key) {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int numRead;
        try {
            do {
                numRead = clientChannel.read(buffer);
                if (numRead > 0) {
                    byte[] data = new byte[numRead];
                    System.arraycopy(buffer.array(), 0, data, 0, numRead);
                    buffer.clear();

                    clientInputBuffer.append(new String(data));
                }
            } while (numRead > 0);

        } catch (IOException e) {
            throw new ServerException("Exception by reading from a client socket.", e);
        }
    }

    private String readLineFromBuffer() {
        if (clientInputBuffer.length() > 0) {
            int newLinePosition = clientInputBuffer.indexOf("\n");
            String line = clientInputBuffer.substring(0, newLinePosition + 1);
            clientInputBuffer.delete(0, newLinePosition + 1);

            return line.trim();
        }
        return null;
    }

    private void closeClientChannel(SelectionKey key) {
        System.out.println("closeClientChannel..............................");
        connectionPool.decrementAndGet();
        try {
            key.cancel();
            ((SocketChannel)key.channel()).socket().close();
            key.channel().close();

        } catch (Exception ignore) {
            log.warn("Exception by closing a client channel.", ignore);
        }
    }

    private void refuseConnectionOverMaxPool(Socket clientSocket) {
// TODO write NIO???
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
    }

    /**
     * Stops the server listener.
     */
    public void stop() {
        log.info("Closing a serverChannel and stopping the server...");

        listening.set(false);

        if (selector != null) {
            try {
                selector.close();
            } catch (IOException e) {
                log.warn("Cannot close a selector", e);
            }
        }

        if (serverChannel != null) {
            try {
                serverChannel.socket().close();
                serverChannel.close();
            } catch (IOException e) {
                log.warn("Cannot close a serverChannel", e);
            }
        }

        dataSource.cleanUpData();
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
