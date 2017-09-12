package cz.net21.ttulka.thistledb.server;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cz.net21.ttulka.thistledb.console.Console;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Application to start/stop a sever from the command line.
 *
 * @author ttulka
 */
@CommonsLog
public final class Application {

    public static void main(String[] args) {
        Options cmdOptions = new Options();
        cmdOptions.addOption("p", "port", true, "Port to listen on.");
        cmdOptions.addOption("d", "dataDir", true, "Data directory to store DB files into.");
        cmdOptions.addOption("m", "max", true, "Maximum client connections.");
        cmdOptions.addOption("h", "help", false, "Help.");

        try {
            CommandLine cmdLine = new DefaultParser().parse(cmdOptions, args);
            if (cmdLine.hasOption("h")) {
                printHelp(cmdOptions);
                System.exit(0);
            }

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

        if (cmdLine.hasOption("m")) {
            int maxClientConnections = Integer.parseInt(cmdLine.getOptionValue("m"));
            server.setMaxClientConnections(maxClientConnections);
        }

        server.start(5000);

        startCommandConsole(server.getPort());

        server.stop(Integer.MAX_VALUE);
    }

    private static void startCommandConsole(int port) {
        try {
            Console console = new Console(port);
            console.start();

        } catch (Throwable t) {
            System.err.println("Error by console running: " + t.getMessage());
            log.error(t);
        }
    }

    private static void printHelp(Options cmdOptions) {
        new HelpFormatter().printHelp(Application.class.getName(), cmdOptions);
    }
}
