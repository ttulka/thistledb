package cz.net21.ttulka.thistledb.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.json.JSONObject;

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

    private final Socket socket;
    private final String query;

    private BufferedReader in;

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
    public JSONObject getNextResult() {
        if (in == null) {
            throw new IllegalStateException("Query was not executed yet.");
        }
        try {
            String result = in.readLine();

            if (result != null && !result.isEmpty()) {

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
                return new JSONObject(result);
            }
            return null;

        } catch (Exception e) {
            throw new ClientException("Error by receiving results from server.", e);
        }
    }

    private JSONObject error(String result) {
        String msg = result.substring(ERROR.length());
        return new JSONObject("{\"status\":\"error\", \"message\":\"" + msg + "\"}");
    }

    private JSONObject invalid(String result) {
        String msg = result.substring(INVALID.length());
        return new JSONObject("{\"status\":\"invalid\", \"message\":\"" + msg + "\"}");
    }

    private JSONObject okay() {
        return new JSONObject("{\"status\":\"okay\"}");
    }
}
