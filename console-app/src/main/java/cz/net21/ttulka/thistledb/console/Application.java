package cz.net21.ttulka.thistledb.console;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ttulka
 * <p>
 * Application to start/stop a console from the command line.
 */
@CommonsLog
public final class Application {

    public static void main(String[] args) {
        Options cmdOptions = new Options();
        cmdOptions.addOption("h", "host", true, "Host to listen on.");
        cmdOptions.addOption("p", "port", true, "Port to listen on.");

        try {
            CommandLine cmdLine = new DefaultParser().parse(cmdOptions, args);

            try (Console console = createConsole(cmdLine)) {
                console.start();
            }
        } catch (NumberFormatException | ParseException e) {
            System.err.println("Cannot parse the user input: " + e.getMessage());
            log.error(e);
            printHelp(cmdOptions);
            System.exit(-1);
        } catch (Throwable t) {
            System.err.println("Error by console running: " + t.getMessage());
            log.error(t);
            System.exit(-1);
        }
        System.exit(0);
    }

    private static Console createConsole(CommandLine cmdLine) {
        Console console;

        if (cmdLine.hasOption("h") && cmdLine.hasOption("p")) {
            String host = cmdLine.getOptionValue("h");
            int port = Integer.parseInt(cmdLine.getOptionValue("p"));
            console = new Console(host, port);
        } else if (cmdLine.hasOption("h")) {
            String host = cmdLine.getOptionValue("h");
            console = new Console(host);
        } else if (cmdLine.hasOption("p")) {
            int port = Integer.parseInt(cmdLine.getOptionValue("p"));
            console = new Console(port);
        } else {
            console = new Console();
        }
        return console;
    }

    private static void printHelp(Options cmdOptions) {
        new HelpFormatter().printHelp(Application.class.getName(), cmdOptions);
    }
}
