package cz.net21.ttulka.thistledb.console;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by ttulka
 * <p>
 * The admin console.
 */
public class Console implements AutoCloseable {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9658;

    private final String host;
    private final int port;

    public Console() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    public Console(String host) {
        this(host, DEFAULT_PORT);
    }

    public Console(int port) {
        this(DEFAULT_HOST, port);
    }

    public Console(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() {
        printLogo();

        // TODO
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String s = null;
        System.out.println("\nType 'quit' to stop the server and exit the console.");
        do {
            try {
                System.out.print("$ ");
                s = br.readLine();
            } catch (Exception e) {
                System.err.println("Invalid user input!");
            }
        } while (s == null || !s.equals("quit"));
    }

    @Override
    public void close() {
        // TODO
    }

    private static void printLogo() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Console.class.getResourceAsStream("/logo.txt")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Throwable t) {
            // ignore
        }
    }
}
