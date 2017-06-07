package cz.net21.ttulka.thistledb.client;

import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by ttulka
 * <p>
 * Client driver.
 */
public class Client implements AutoCloseable {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9658;

    private final Socket socket;

    public Client() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    public Client(String host) {
        this(host, DEFAULT_PORT);
    }

    public Client(int port) {
        this(DEFAULT_HOST, port);
    }

    public Client(String host, int port) {
        try {
            this.socket = new Socket(host, port);

        } catch (Exception e) {
            throw new ClientException("Cannot open a socket on " + host + ":" + port + ".", e);
        }
    }

    public JsonPublisher executeQuery(Query query) {
        checkQuery(query);
        return executeQuery(query.getNativeQuery());
    }

    public JsonPublisher executeQuery(String nativeQuery) {
        checkQuery(nativeQuery);

        // TODO this is just an inspiration
//        try (PrintWriter out = new PrintWriter(socket.getOutputStream())
//        ) {
//            out.println(command);
//
//        } catch (Exception e) {
//            throw new ClientException("Cannot send a command to socket.", e);
//        }
        return null;
    }

    public void executeCommand(Query query) {
        checkQuery(query);
        executeCommand(query.getNativeQuery());
    }

    public void executeCommand(String nativeQuery) {
        checkQuery(nativeQuery);

        try (PrintWriter out = new PrintWriter(socket.getOutputStream())) {
            out.println(nativeQuery);

        } catch (Exception e) {
            throw new ClientException("Cannot send a command [" + nativeQuery + "] to socket.", e);
        }
    }

    private void checkQuery(Query query) {
        if (query == null) {
            throw new NullPointerException("Query cannot be null.");
        }
    }

    private void checkQuery(String query) {
        if (query == null) {
            throw new NullPointerException("Query cannot be null.");
        }
        if (query.isEmpty()) {
            throw new IllegalArgumentException("Query cannot be empty.");
        }
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (Throwable t) {
            throw new ClientException("Cannot close a socket.", t);
        }
    }
}
