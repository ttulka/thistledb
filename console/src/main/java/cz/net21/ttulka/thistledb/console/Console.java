package cz.net21.ttulka.thistledb.console;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import cz.net21.ttulka.thistledb.client.Client;
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

        String s = null;
        out.println("\nType 'quit' to stop the server and exit the console.");
        do {
            try {
                out.print("$ ");
                s = br.readLine();

                // TODO call the client to execute the query

            } catch (Exception e) {
                err.println("Invalid user input!");
            }
        } while (s == null || !s.equals("quit"));
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
