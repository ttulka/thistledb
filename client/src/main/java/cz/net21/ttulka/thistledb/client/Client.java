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

    /**
     * Executes a query, in the reactive style.
     *
     * @param query the query to be executed
     * @return the reactive result publisher
     * @throws ClientException if the query cannot be sent to socket
     */
    public JsonPublisher executeQuery(Query query) {
        return executeQuery(query.getNativeQuery());
    }

    /**
     * Executes a query, wait for a response.
     *
     * @param query the query to be executed
     * @return the result
     * @throws ClientException if the query cannot be sent to server
     */
    public List<JSONObject> executeQueryBlocking(Query query) {
        return executeQueryBlocking(query.getNativeQuery());
    }

    /**
     * Executes a query, in the reactive style.
     *
     * @param nativeQuery the native query to be executed
     * @return the reactive result publisher
     * @throws ClientException if the query cannot be sent to server
     */
    public JsonPublisher executeQuery(String nativeQuery) {
        checkQuery(nativeQuery);
        return new JsonPublisher(new QueryExecutor(socket, nativeQuery));
    }

    /**
     * Executes a query, wait for a response.
     *
     * @param nativeQuery the native query to be executed
     * @return the result
     * @throws ClientException if the query cannot be sent to server
     */
    public List<JSONObject> executeQueryBlocking(String nativeQuery) {
        checkQuery(nativeQuery);
        return executeProcessorBlocking(new QueryExecutor(socket, nativeQuery));
    }

    /**
     * Executes a command, doesn't wait for a response.
     *
     * @param query the query command to be executed
     * @throws ClientException if the command cannot be sent to socket
     */
    public void executeCommand(Query query) {
        checkQuery(query);
        executeCommand(query.getNativeQuery());
    }

    /**
     * Sends a command to the server, doesn't wait for a response.
     *
     * @param nativeQuery the native query command to be executed
     * @throws ClientException if the command cannot be sent to socket
     */
    public void executeCommand(String nativeQuery) {
        checkQuery(nativeQuery);
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            out.println(nativeQuery);

        } catch (Exception e) {
            throw new ClientException("Cannot send a command [" + nativeQuery + "] to socket.", e);
        }
    }

    private void executeProcessor(QueryExecutor queryExecutor) {
        new Thread(() -> {
            queryExecutor.executeQuery();
            queryExecutor.getNextResult();
        }).start();
    }

    private List<JSONObject> executeProcessorBlocking(QueryExecutor queryExecutor) {
        queryExecutor.executeQuery();

        List<JSONObject> toReturn = new ArrayList<>();
        JSONObject json;
        while ((json = queryExecutor.getNextResult()) != null) {
            toReturn.add(json);
        }
        return toReturn;
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
     * Close the client connection to the server.
     *
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
