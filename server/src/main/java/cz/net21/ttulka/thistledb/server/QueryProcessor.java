package cz.net21.ttulka.thistledb.server;

import java.io.PrintWriter;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import cz.net21.ttulka.thistledb.tson.TSONObject;
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
    public static final String FINISHED = "FINISHED";
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
            processQuery(parser).subscribe(
                    result -> out.println(result),
                    error -> {
                        log.error("Error by executing a query: " + input + ".", error);
                        printEOF(out, ERROR + " " + error.getMessage());
                    },
                    () -> printEOF(out, FINISHED)
            );

        } catch (IllegalArgumentException e) {
            printEOF(out, INVALID + " " + e.getMessage());
        } catch (Exception e) {
            log.error("Error by processing a command: " + input + ".", e);
            printEOF(out, ERROR + " " + e.getMessage());
        }
    }

    private void printEOF(PrintWriter out, String message) {
        try {
            out.println(message);
            out.println();
            out.flush();
        } catch (Throwable t) {
            // ignore
        }
    }

    protected String acceptQuery(@NonNull Commands command) {
        return ACCEPTED + " " + command;
    }

    protected Flux<String> processQuery(@NonNull QueryParser parser) {
        Commands command = parser.getCommand();

        switch (command) {
            case SELECT:
                return processSelect(parser);
            case INSERT:
                return processInsert(parser);
            case UPDATE:
                return processUpdate(parser);
            case DELETE:
                return processDelete(parser);
            case CREATE:
                return processCreate(parser);
            case DROP:
                return processDrop(parser);
            case CREATE_INDEX:
                return processCreateIndex(parser);
            case DROP_INDEX:
                return processDropIndex(parser);
            default:
                throw new RuntimeException("Cannot process command " + command);
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

        dataSource.insert(collection, new TSONObject(values));
        return Flux.just(OKAY);
    }

    Flux<String> processUpdate(QueryParser parser) {
        String collection = parser.parseCollection();
        String where = parser.parseWhere();

        String[] columns = parser.parseSetColumns();
        String[] values = parser.parseSetValues();

        dataSource.update(collection, columns, values, where);
        return Flux.just(OKAY);
    }

    Flux<String> processDelete(QueryParser parser) {
        String collection = parser.parseCollection();
        String where = parser.parseWhere();

        dataSource.delete(collection, where);
        return Flux.just(OKAY);
    }

    Flux<String> processCreate(QueryParser parser) {
        String collection = parser.parseCollection();

        dataSource.createCollection(collection);
        return Flux.just(OKAY);
    }

    Flux<String> processDrop(QueryParser parser) {
        String collection = parser.parseCollection();

        dataSource.dropCollection(collection);
        return Flux.just(OKAY);
    }

    Flux<String> processCreateIndex(QueryParser parser) {
        String collection = parser.parseCollection();
        String columns = parser.parseColumns();

        dataSource.createIndex(collection, columns);
        return Flux.just(OKAY);
    }

    Flux<String> processDropIndex(QueryParser parser) {
        String collection = parser.parseCollection();
        String columns = parser.parseColumns();

        dataSource.dropIndex(collection, columns);
        return Flux.just(OKAY);
    }

    private String serialize(TSONObject json) {
        if (json == null) {
            return "{}";
        }
        return json.toString();
    }
}
