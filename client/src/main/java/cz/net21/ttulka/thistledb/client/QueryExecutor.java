package cz.net21.ttulka.thistledb.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Processing a query.
 * <p>
 * Uses blocking IO approach to offer both blocking and non-blocking (via Reactive Streams) variants.
 *
 * @author ttulka
 */
class QueryExecutor {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String ERROR = "ERROR";
    public static final String INVALID = "INVALID";
    public static final String OKAY = "OKAY";
    public static final String FINISHED = "FINISHED";

    private final Socket serverSocket;
    private final String query;

    private BufferedReader in;

    private boolean listening = true;

    /**
     * Creates a {@link QueryExecutor}
     *
     * @param serverSocket the server serverSocket
     * @param nativeQuery  the query
     * @throws QueryException when the query is invalid
     */
    public QueryExecutor(Socket serverSocket, String nativeQuery) {
        this.serverSocket = serverSocket;
        this.query = cleanQuery(nativeQuery);

        if (!QueryValidator.validate(query)) {
            throw new QueryException("Query '" + query + "' is invalid.");
        }
    }

    private String cleanQuery(String query) {
        query = query.trim();
        if (query.endsWith(";")) {
            query = query.substring(0, query.length() - 1);
        }
        return query.replace("\\n", " ");   // remove new lines
    }

    /**
     * Executes a query.
     *
     * @throws ClientException when execution goes wrong
     */
    public void executeQuery() {
        try {
            PrintWriter out = new PrintWriter(serverSocket.getOutputStream(), true);
            out.println(query);

            in = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));

        } catch (Exception e) {
            throw new ClientException("Cannot send a query to serverSocket: " + query, e);
        }
    }

    /**
     * Retrieves a result of the query.
     *
     * @return the result
     * @throws ClientException       when retrieving goes wrong
     * @throws IllegalStateException when the query was not executed yet
     */
    public String getNextResult() {
        if (in == null) {
            throw new IllegalStateException("Query was not executed yet.");
        }
        try {
            while (listening) {
                String result = in.readLine();

                if (result != null && !result.isEmpty()) {

                    if (result.equals(FINISHED)) {
                        stopListening();
                        return null;
                    }
                    if (result.startsWith(ACCEPTED)) {
                        return getNextResult();
                    }
                    if (result.startsWith(ERROR)) {
                        stopListening();
                        return error(result);
                    }
                    if (result.startsWith(INVALID)) {
                        stopListening();
                        return invalid(result);
                    }
                    if (result.startsWith(OKAY)) {
                        in.readLine();  // FINISHED
                        stopListening();
                        return okay();
                    }
                    return result;
                }
            }
        } catch (Exception e) {
            throw new ClientException("Error by receiving results from server.", e);
        }
        return null;
    }

    private void stopListening() throws IOException {
        listening = false;
    }

    private String error(String result) {
        String msg = result.substring(ERROR.length() + 1);
        return "{\"status\":\"error\", \"message\":\"" + removeQuotes(msg) + "\"}";
    }

    private String invalid(String result) {
        String msg = result.substring(INVALID.length() + 1);
        return "{\"status\":\"invalid\", \"message\":\"" + removeQuotes(msg) + "\"}";
    }

    private String okay() {
        return "{\"status\":\"okay\"}";
    }

    private String removeQuotes(String s) {
        return s.replace("\"", "'");
    }
}
