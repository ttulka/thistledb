package cz.net21.ttulka.thistledb.server;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ttulka
 * <p>
 * Application application to start/stop a sever from the command line.
 */
@CommonsLog
public final class Application {

    public static void main(String[] args) {
        Options cmdOptions = new Options();
        cmdOptions.addOption("p", "port", true, "Port to listen on.");
        cmdOptions.addOption("d", "dataDir", true, "Data directory to store DB files into.");

        try {
            CommandLine cmdLine = new DefaultParser().parse(cmdOptions, args);

            startServer(cmdLine);

        } catch (NumberFormatException | ParseException e) {
            System.err.println("Cannot parse the user input: " + e.getMessage());
            log.error(e);
            printHelp(cmdOptions);
            System.exit(-1);
        } catch (Throwable t) {
            System.err.println("Error by server running: " + t.getMessage());
            log.error(t);
            System.exit(-1);
        }
        System.exit(0);
    }

    private static void startServer(CommandLine cmdLine) {
        Server server;

        if (cmdLine.hasOption("p") && cmdLine.hasOption("d")) {
            int port = Integer.parseInt(cmdLine.getOptionValue("p"));
            Path dataDir = Paths.get(cmdLine.getOptionValue("d"));
            server = new Server(port, dataDir);
        } else if (cmdLine.hasOption("p")) {
            int port = Integer.parseInt(cmdLine.getOptionValue("p"));
            server = new Server(port);
        } else if (cmdLine.hasOption("d")) {
            Path dataDir = Paths.get(cmdLine.getOptionValue("d"));
            server = new Server(dataDir);
        } else {
            server = new Server();
        }

        printLogo();

        server.startAndWait(5000);

        startCommandConsole();

        server.stop();
    }

    private static void startCommandConsole() {
        // TODO implement the console project and use it here
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

        System.out.println("\nBye!");
    }

    private static void printHelp(Options cmdOptions) {
        new HelpFormatter().printHelp(Application.class.getName(), cmdOptions);
    }

    private static void printLogo() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Application.class.getResourceAsStream("/logo.txt")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Throwable t) {
            // ignore
        }
    }
}
