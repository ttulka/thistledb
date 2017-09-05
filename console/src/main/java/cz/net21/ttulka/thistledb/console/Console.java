package cz.net21.ttulka.thistledb.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

import cz.net21.ttulka.thistledb.client.Client;
import cz.net21.ttulka.thistledb.client.ClientException;
import lombok.extern.apachecommons.CommonsLog;

/**
 * The command-line console.
 * <p>
 * @author ttulka
 */
@CommonsLog
public class Console {

    static final String COMMAND_DELIMITER = ";";
    static final String COMMAND_SEPARATOR = "$ ";
    static final String COMMAND_NEWLINE = "  > ";
    static final String COMMAND_EXIT = "EXIT";

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

    public final void start() {
        printLogo();

        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        out.println("\nType 'quit' to exit.");
        do {
            try {
                out.print(COMMAND_SEPARATOR);

                String command = readCommand(br);
                if (command == null) {
                    continue;
                }
                if (COMMAND_EXIT.equals(command)) {
                    break;
                }
                executeQuery(command);

            } catch (IOException e) {
                err.println("Invalid user input!");
            } catch (ClientException e) {
                err.println("Client error: " + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while (true);

        out.println("\nBye!");
    }

    private String readCommand(BufferedReader br) throws IOException {
        String line;
        boolean newCommand = true;
        StringBuilder command = new StringBuilder();
        do {
            if (!newCommand) {
                if (command.length() == 0) {
                    return null;
                }
                out.print(COMMAND_NEWLINE);
            }
            newCommand = false;

            line = br.readLine();

            if (command.length() == 0 && ("exit".equals(line) || "quit".equals(line))) {
                return COMMAND_EXIT;
            }

            if (line != null && !line.trim().isEmpty()) {
                command.append(" ").append(line.trim());
            }
        } while (!command.toString().endsWith(COMMAND_DELIMITER));

        return command.toString().trim();
    }

    void executeQuery(String query) {
        List<String> result = client.executeQueryBlocking(query);

        out.println("\nResult(s):");

        if (result.isEmpty()) {
            result.add("{}");
        }

        Iterator<String> iterator = result.iterator();
        while (iterator.hasNext()) {
            formatJsonResult(iterator.next(), !iterator.hasNext());
        }
    }

    private void formatJsonResult(String json, boolean last) {
        new JsonFormatter(json, out).formatJsonResult(last);
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
