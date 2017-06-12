package cz.net21.ttulka.thistledb.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;

import org.json.JSONObject;

import cz.net21.ttulka.thistledb.client.Client;
import cz.net21.ttulka.thistledb.client.ClientException;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ttulka
 * <p>
 * The admin console.
 */
@CommonsLog
public class Console implements AutoCloseable {

    private final Client client;

    private final InputStream in;
    private final PrintStream out;
    private final PrintStream err;

    public Console() {
        this(Client.DEFAULT_HOST, Client.DEFAULT_PORT);
    }

    public Console(String host) {
        this(host, Client.DEFAULT_PORT);
    }

    public Console(int port) {
        this(Client.DEFAULT_HOST, port);
    }

    public Console(String host, int port) {
        this(host, port, System.in, System.out, System.err);
    }

    public Console(String host, int port, InputStream in, PrintStream out, PrintStream err) {
        this.in = in;
        this.out = out;
        this.err = err;

        this.client = new Client(host, port);
    }

    public void start() {
        printLogo();

        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String command = null;
        out.println("\nType 'quit' to stop the server and exit the console.");
        do {
            try {
                out.print("$ ");
                command = br.readLine();
                if (command != null && !command.trim().isEmpty()) {
                    command = command.trim();

                    if ("quit".equals(command)) {
                        break;
                    }

                    executeQuery(command);
                }
            } catch (IOException e) {
                err.println("Invalid user input!");
            } catch (ClientException e) {
                err.println("Client error: " + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while (command != null);

        out.println("\nBye!");
    }

    void executeQuery(String query) {
        List<JSONObject> result = client.executeQueryBlocking(query);

        out.println("\nResult(s):");
        result.forEach(this::formatJsonResult);
        out.println();
    }

    void formatJsonResult(JSONObject json) {
        // TODO
        out.println(json);
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (Exception e) {
            err.println("Exception by closing the client: " + e.getMessage());
            log.error(e);
        }
    }

    private void printLogo() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Console.class.getResourceAsStream("/logo.txt")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                out.println(line);
            }
        } catch (Throwable t) {
            // ignore
        }
    }
}
