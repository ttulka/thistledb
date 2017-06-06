package cz.net21.ttulka.thistledb.server;

import java.io.PrintWriter;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ttulka
 * <p>
 * Processing a request.
 */
@CommonsLog
public class Processor {

    public static final String ACCEPTED = "ACCEPTED";

    private final DataSource dataSource;

    public Processor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void process(@NonNull String input, @NonNull PrintWriter out) {
        try {
            Commands command = parseCommand(input);

            acceptCommand(command, out);

            // TODO process the command

            out.flush();

        } catch (Exception e) {
            log.error("Error by processing a command: " + input + ".", e);
        }
    }

    protected Commands parseCommand(String input) {
        input += " ";
        String command = input.substring(0, input.indexOf(" "));
        return Commands.valueOf(command.toUpperCase());
    }

    protected void acceptCommand(Commands command, PrintWriter out) {
        out.println(ACCEPTED + " " + command);
    }
}
