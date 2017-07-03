package cz.net21.ttulka.thistledb.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by ttulka
 * <p>
 * Processing a query.
 */
class QueryExecutor {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String ERROR = "ERROR";
    public static final String INVALID = "INVALID";
    public static final String OKAY = "OKAY";
    public static final String FINISHED = "FINISHED";

    private final Socket socket;
    private final String query;

    private BufferedReader in;

    private boolean listening = true;

    public QueryExecutor(Socket socket, String nativeQuery) {
        this.socket = socket;
        this.query = nativeQuery;
    }

    /**
     * Executes a query.
     *
     * @throws ClientException when execution goes wrong
     */
    public void executeQuery() {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(query);

            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        } catch (Exception e) {
            throw new ClientException("Cannot send a query to socket: " + query, e);
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
                        return null;
                    }
                    if (result.startsWith(ACCEPTED)) {
                        return getNextResult();
                    }
                    if (result.startsWith(ERROR)) {
                        return error(result);
                    }
                    if (result.startsWith(INVALID)) {
                        return invalid(result);
                    }
                    if (result.startsWith(OKAY)) {
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

    private String error(String result) {
        listening = false;
        String msg = result.substring(ERROR.length() + 1);
        return "{\"status\":\"error\", \"message\":\"" + removeQuotes(msg) + "\"}";
    }

    private String invalid(String result) {
        listening = false;
        String msg = result.substring(INVALID.length() + 1);
        return "{\"status\":\"invalid\", \"message\":\"" + removeQuotes(msg) + "\"}";
    }

    private String okay() {
        listening = false;
        return "{\"status\":\"okay\"}";
    }

    private String removeQuotes(String s) {
        return s.replace("\"", "'");
    }
}
