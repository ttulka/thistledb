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
        cmdOptions.addOption("c", "cacheExpirationTime", true, "Cache expiration time (in minutes).");
        cmdOptions.addOption("m", "maxConnections", true, "Maximum client connections.");
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
        Server.ServerBuilder builder = Server.builder();

        if (cmdLine.hasOption("p")) {
            int port = Integer.parseInt(cmdLine.getOptionValue("p"));
            builder.port(port);
        }
        if (cmdLine.hasOption("d")) {
            Path dataDir = Paths.get(cmdLine.getOptionValue("d"));
            builder.dataDir(dataDir);
        }
        if (cmdLine.hasOption("c")) {
            int cacheExpirationTime = Integer.parseInt(cmdLine.getOptionValue("c"));
            builder.cacheExpirationTime(cacheExpirationTime);
        }

        Server server = builder.build();

        if (cmdLine.hasOption("m")) {
            int maxClientConnections = Integer.parseInt(cmdLine.getOptionValue("m"));
            server.setMaxClientConnections(maxClientConnections);
        }

        server.start(5000);

        startCommandConsole(server.getPort());

        server.stop();
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
