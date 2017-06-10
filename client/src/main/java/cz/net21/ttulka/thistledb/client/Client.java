package cz.net21.ttulka.thistledb.client;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

/**
 * Created by ttulka
 * <p>
 * Client driver.
 */
public class Client implements AutoCloseable {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9658;

    private final Socket socket;

    /**
     * @throws ClientException if a socket cannot be opened
     */
    public Client() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    /**
     * @throws ClientException if a socket cannot be opened
     */
    public Client(String host) {
        this(host, DEFAULT_PORT);
    }

    /**
     * @throws ClientException if a socket cannot be opened
     */
    public Client(int port) {
        this(DEFAULT_HOST, port);
    }

    /**
     * @throws ClientException if a socket cannot be opened
     */
    public Client(String host, int port) {
        try {
            this.socket = new Socket(host, port);

        } catch (Exception e) {
            throw new ClientException("Cannot open a socket on " + host + ":" + port + ".", e);
        }
    }

    public JsonPublisher executeQuery(Query query) {
        return executeQuery(query.getNativeQuery());
    }

    public List<JSONObject> executeQueryBlocking(Query query) {
        return executeQueryBlocking(query.getNativeQuery());
    }

    public JsonPublisher executeQuery(String nativeQuery) {
        checkQuery(nativeQuery);
        return new JsonPublisher(new Processor(socket, nativeQuery));
    }

    public List<JSONObject> executeQueryBlocking(String nativeQuery) {
        checkQuery(nativeQuery);
        return executeProcessorBlocking(new Processor(socket, nativeQuery));
    }

    public void executeCommand(Query query) {
        checkQuery(query);
        executeCommand(query.getNativeQuery());
    }

    private void executeProcessor(Processor processor) {
        new Thread(() -> {
            processor.executeQuery();
            processor.getNextResult();
        }).start();
    }

    private List<JSONObject> executeProcessorBlocking(Processor processor) {
        processor.executeQuery();

        List<JSONObject> toReturn = new ArrayList<>();
        JSONObject json;
        while ((json = processor.getNextResult()) != null) {
            toReturn.add(json);
        }
        return toReturn;
    }

    /**
     * @throws ClientException if a command cannot be sent to socket
     */
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

    /**
     * @throws ClientException if a socket cannot be closed
     */
    @Override
    public void close() {
        try {
            socket.close();
        } catch (Throwable t) {
            throw new ClientException("Cannot close a socket.", t);
        }
    }
}
