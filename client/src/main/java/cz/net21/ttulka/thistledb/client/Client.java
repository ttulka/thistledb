package cz.net21.ttulka.thistledb.client;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Client driver.
 *
 * @author ttulka
 */
public class Client implements AutoCloseable {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9658;

    private final Socket serverSocket;

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
        try {
            serverSocket = new Socket(host, port);
            serverSocket.setSoTimeout(timeout);

        } catch (IOException e) {
            throw new ClientException("Cannot open a socket on " + host + ":" + port + ".", e);
        }
    }

    /**
     * Close the client.
     *
     * @throws ClientException when an exception occurs by closing
     */
    @Override
    public void close() {
        try {
            serverSocket.close();

        } catch (IOException e) {
            throw new ClientException("Cannot close a socket to " + serverSocket.getRemoteSocketAddress() + ".", e);
        }
    }

    /**
     * Gets timeout for a client response.
     *
     * @return the timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Sets timeout for a client response. The timeout must be greater then zero. A timeout of zero is interpreted as an infinite timeout.
     *
     * @param timeout the specified timeout, in milliseconds
     * @throws ClientException          when the client is closed
     * @throws IllegalArgumentException when the timeout value is less than zero
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
        try {
            serverSocket.setSoTimeout(timeout);

        } catch (SocketException e) {
            throw new ClientException("Client is closed.", e);
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
        return new JsonPublisher(new QueryExecutor(serverSocket, nativeQuery));
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
        return executeProcessorBlocking(new QueryExecutor(serverSocket, nativeQuery));
    }

    /**
     * Executes a command, doesn't wait for a response.
     *
     * @param query    the query command to be executed
     * @param consumer the response consumer
     * @throws ClientException if the command cannot be sent to socket
     * @throws ClientException if the command returns more than one response
     */
    public void executeCommand(Query query, Consumer<String> consumer) {
        checkQuery(query);
        executeCommand(query.getNativeQuery(), consumer);
    }

    /**
     * Sends a command to the server, doesn't wait for a response.
     *
     * @param nativeQuery the native query command to be executed
     * @param consumer    the response consumer
     * @throws ClientException if the command cannot be sent to socket
     * @throws ClientException if the command returns more than one response
     */
    public void executeCommand(String nativeQuery, Consumer<String> consumer) {
        checkQuery(nativeQuery);
        new Thread(() -> {
            QueryExecutor queryExecutor = new QueryExecutor(serverSocket, nativeQuery);
            queryExecutor.executeQuery();
            String response = queryExecutor.getNextResult();  // status response

            if (queryExecutor.getNextResult() != null) {
                throw new ClientException("Command cannot return more than one response.");
            }
            if (consumer != null) {
                consumer.accept(response);
            }
        }).start();
    }

    /**
     * Executes a command, waits for a response.
     *
     * @param query the query command to be executed
     * @return the server response
     * @throws ClientException if the command cannot be sent to socket
     * @throws ClientException if the command returns more than one response
     */
    public String executeCommandBlocking(Query query) {
        checkQuery(query);
        return executeCommandBlocking(query.getNativeQuery());
    }

    /**
     * Sends a command to the server, waits for a response.
     *
     * @param nativeQuery the native query command to be executed
     * @return the server response
     * @throws ClientException if the command cannot be sent to socket
     * @throws ClientException if the command returns more than one response
     */
    public String executeCommandBlocking(String nativeQuery) {
        checkQuery(nativeQuery);
        QueryExecutor queryExecutor = new QueryExecutor(serverSocket, nativeQuery);
        queryExecutor.executeQuery();
        String response = queryExecutor.getNextResult();

        if (queryExecutor.getNextResult() != null) {
            throw new ClientException("Command cannot return more than one response.");
        }
        return response;
    }

    /**
     * Tests a connection.
     *
     * @return true if connection successfully created, otherwise false
     */
    public boolean test() {
        final String testQuery = "SELECT name FROM DUAL";
        try {
            List<String> result = executeProcessorBlocking(new QueryExecutor(serverSocket, testQuery));
            return result != null && result.size() == 1;

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
