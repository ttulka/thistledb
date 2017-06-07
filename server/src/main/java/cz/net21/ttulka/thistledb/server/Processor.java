package cz.net21.ttulka.thistledb.server;

import java.io.PrintWriter;
import java.util.Collection;

import org.json.JSONObject;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;
import reactor.core.publisher.Flux;

/**
 * Created by ttulka
 * <p>
 * Processing a request.
 */
@CommonsLog
public class Processor {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String ERROR = "ERROR";
    public static final String QL_ERROR = "QL_ERROR";

    private final DataSource dataSource;

    public Processor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void process(@NonNull String input, @NonNull PrintWriter out) {
        try {
            out.println(ACCEPTED);

            processCommand(input).subscribe(
                    result -> out.println(result),
                    error -> log.error("Error by executing a command: " + input + ".", error),
                    () -> out.flush()
            );

        } catch (Exception e) {
            log.error("Error by processing a command: " + input + ".", e);
        }
    }

    protected Commands parseCommand(@NonNull String input) {
        input += " ";
        String command = input.substring(0, input.indexOf(" "));
        return Commands.valueOf(command.toUpperCase());
    }

    protected String acceptCommand(@NonNull Commands command) {
        return ACCEPTED + " " + command;
    }

    protected Flux<String> processCommand(@NonNull String input) {
        try {
            Commands command = parseCommand(input);

            switch (command) {
                case SELECT:
                    return processSelect(input);
                // TODO next commands
                default:
                    return Flux.just(ERROR + " invalid command: " + command);
            }
        } catch (Exception e) {
            log.error("Error by processing a command [" + input + "].", e);
            return Flux.just(ERROR);
        }
    }

    Flux<String> processSelect(String input) {
        String collection = parseCollection(input);
        String columns = parseColumns(input);
        String where = parseWhere(input);

        return dataSource.select(collection, columns, where)
                .map(this::serialize);
    }

    String parseCollection(String input) {
        input = input.substring(input.toLowerCase().indexOf("from") + 5).trim();
        if (input.indexOf(" ") > 0) {
            input = input.substring(0, input.indexOf(" "));
        }
        return input;
    }

    String parseColumns(String input) {
        input = input.substring(input.indexOf(" "), input.toLowerCase().indexOf("from")).replace(" ", "");
        return input;
    }

    String parseWhere(String input) {
        input = input.substring(input.toLowerCase().indexOf("where") + 5).trim();
        return input;
    }

    private String serialize(JSONObject json) {
        if (json == null) {
            return "{}";
        }
        return json.toString();
    }

    private String serialize(Collection<JSONObject> jsonList) {
        if (jsonList.isEmpty()) {
            return "{}";
        }
        if (jsonList.size() == 1) {
            return serialize(jsonList.iterator().next());
        }
        StringBuilder sb = new StringBuilder();
        jsonList.stream()
                .map(this::serialize)
                .forEach(s -> sb.append(s).append(","));
        sb.deleteCharAt(sb.length() - 1);
        return "{[" + sb + "]}";
    }
}
