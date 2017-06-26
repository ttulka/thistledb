package cz.net21.ttulka.thistledb.client;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ttulka
 * <p>
 * Client driver.
 */
public class Client {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9658;

    private final String host;
    private final int port;

    private int timeout = 2 * 60 * 1000;    // 2 minutes

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
        this.host = host;
        this.port = port;
    }

    /**
     * Sets timeout for a client response.
     * The timeout must be greater then zero. A timeout of zero is interpreted as an infinite timeout.
     *
     * @param timeout the specified timeout, in milliseconds
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
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
    public List<String> executeQueryBlocking(Query query) {
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
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            return new JsonPublisher(new QueryExecutor(socket, nativeQuery));

        } catch (IOException e) {
            throw new ClientException("Cannot open a socket on " + host + ":" + port + ".", e);
        }
    }

    /**
     * Executes a query, wait for a response.
     *
     * @param nativeQuery the native query to be executed
     * @return the result
     * @throws ClientException if the query cannot be sent to server
     */
    public List<String> executeQueryBlocking(String nativeQuery) {
        checkQuery(nativeQuery);
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            return executeProcessorBlocking(new QueryExecutor(socket, nativeQuery));

        } catch (IOException e) {
            throw new ClientException("Cannot open a socket on " + host + ":" + port + ".", e);
        }
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
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            new Thread(() -> {
                new QueryExecutor(socket, nativeQuery).executeQuery();
            }).start();
        } catch (IOException e) {
            throw new ClientException("Cannot open a socket on " + host + ":" + port + ".", e);
        } catch (Exception e) {
            throw new ClientException("Cannot send a command [" + nativeQuery + "] to socket.", e);
        }
    }

    /**
     * Tests a connection.
     *
     * @return true if connection successfully created, otherwise false
     */
    public boolean test() {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            return true;

        } catch (Throwable t) {
            return false;
        }
    }

    private List<String> executeProcessorBlocking(QueryExecutor queryExecutor) {
        queryExecutor.executeQuery();

        List<String> toReturn = new ArrayList<>();
        String json;
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
}
