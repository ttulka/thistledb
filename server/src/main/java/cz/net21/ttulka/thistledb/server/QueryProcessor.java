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
class QueryProcessor {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String ERROR = "ERROR";
    public static final String INVALID = "INVALID";
    public static final String OKAY = "OKAY";

    private final DataSource dataSource;

    public QueryProcessor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void process(@NonNull String input, @NonNull PrintWriter out) {
        if (log.isDebugEnabled()) {
            log.debug("Processing a client request: " + input);
        }
        try {
            out.println(ACCEPTED);

            QueryParser parser = new QueryParser(input);

            if (parser.validate()) {

                processQuery(parser).subscribe(
                        result -> out.println(result),
                        error -> log.error("Error by executing a query: " + input + ".", error)
                );
            } else {
                out.println(INVALID);
            }

        } catch (Exception e) {
            log.error("Error by processing a command: " + input + ".", e);
        } finally {
            try {
                out.println();
                out.flush();
            } catch (Throwable t) {
                // ignore
            }
        }
    }

    protected String acceptQuery(@NonNull Commands command) {
        return ACCEPTED + " " + command;
    }

    protected Flux<String> processQuery(@NonNull QueryParser parser) {
        try {
            Commands command = parser.parseCommand();

            switch (command) {
                case SELECT:
                    return processSelect(parser);
                case INSERT:
                    return processInsert(parser);
                // TODO next commands
                default:
                    return Flux.just(ERROR + " invalid command: " + command);
            }
        } catch (Exception e) {
            log.error("Cannot process a command [" + parser.getQuery() + "].", e);
            return Flux.just(ERROR + " " + e.getMessage());
        }
    }

    Flux<String> processSelect(QueryParser parser) {
        String collection = parser.parseCollection();
        String columns = parser.parseColumns();
        String where = parser.parseWhere();

        return dataSource.select(collection, columns, where)
                .map(this::serialize);
    }

    Flux<String> processInsert(QueryParser parser) {
        String collection = parser.parseCollection();
        String values = parser.parseValues();

        dataSource.insert(collection, new JSONObject(values));
        return Flux.just(OKAY);
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
