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
class Processor implements AutoCloseable {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String ERROR = "ERROR";
    public static final String INVALID = "INVALID";
    public static final String OKAY = "OKAY";

    private final Socket socket;
    private final String query;

    private BufferedReader in;  // TODO could be only Reader?

    public Processor(Socket socket, String nativeQuery) {
        this.socket = socket;
        this.query = nativeQuery;
    }

    public void executeQuery() {
        try (PrintWriter out = new PrintWriter(socket.getOutputStream())) {
            out.println(query);

        } catch (Exception e) {
            throw new ClientException("Cannot send a query to socket: " + query, e);
        }

        try {
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (Exception e) {
            throw new ClientException("Cannot read from socket: " + query, e);
        }
    }

    public JSONObject getNextResult() {
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

    @Override
    public void close() throws Exception {
        if (in != null) {
            try {
                in.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
