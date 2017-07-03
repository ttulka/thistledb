package cz.net21.ttulka.thistledb.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;

/**
 * Created by ttulka
 */
@CommonsLog
class QueryParser {

    private static final String COLLECTION = "[\\w\\d]+";

    private static final String JSON = "\\{.*\\}";    // TODO

    private static final String JSON_PATH = "[\\w\\d\\._\\-$]+";    // TODO

    private static final String WHERE = "(" + JSON_PATH + "\\s*=\\s*((['\"](.+)['\"])|(\\d+)))+";
    private static final String WHERE_COMPOSITED = "(" + WHERE + ")(\\s+(AND|OR)\\s+(" + WHERE + "))*";

    static final Pattern SELECT = compile("SELECT\\s+(\\*|" + JSON_PATH + ")\\s+FROM\\s+(" + COLLECTION + ")(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern INSERT = compile("INSERT\\s+INTO\\s+(" + COLLECTION + ")\\s+VALUES\\s+(" + JSON + ")", CASE_INSENSITIVE);
    static final Pattern UPDATE = compile("UPDATE\\s+(" + COLLECTION + ")\\s+SET((\\s+((?!.WHERE).)+)\\s*=\\s*(((?!.WHERE).)+))(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern DELETE = compile("DELETE\\s+FROM\\s+(" + COLLECTION + ")(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern CREATE = compile("CREATE\\s+(?!.*INDEX)(" + COLLECTION + ")", CASE_INSENSITIVE);
    static final Pattern DROP = compile("DROP\\s+((?!.*INDEX)" + COLLECTION + ")", CASE_INSENSITIVE);
    static final Pattern CREATE_INDEX = compile("CREATE\\s+INDEX\\s+(" + JSON_PATH + ")\\s+ON\\s+(" + COLLECTION + ")", CASE_INSENSITIVE);
    static final Pattern DROP_INDEX = compile("DROP\\s+INDEX\\s+(" + JSON_PATH + ")\\s+ON\\s+(" + COLLECTION + ")", CASE_INSENSITIVE);

    private final String ql;

    private final Commands command;

    /**
     * @param ql the query to parse
     * @throws IllegalArgumentException if the query is invalid
     */
    public QueryParser(@NonNull String ql) {
        this.ql = ql.trim();
        this.command = parseCommand(this.ql);
    }

    public String getQuery() {
        return ql;
    }

    public Commands getCommand() {
        return command;
    }

    static Commands parseCommand(String input) {
        if (SELECT.matcher(input).matches()) {
            return Commands.SELECT;
        }
        if (INSERT.matcher(input).matches()) {
            return Commands.INSERT;
        }
        if (UPDATE.matcher(input).matches()) {
            return Commands.UPDATE;
        }
        if (DELETE.matcher(input).matches()) {
            return Commands.DELETE;
        }
        if (CREATE.matcher(input).matches()) {
            return Commands.CREATE;
        }
        if (DROP.matcher(input).matches()) {
            return Commands.DROP;
        }
        if (CREATE_INDEX.matcher(input).matches()) {
            return Commands.CREATE_INDEX;
        }
        if (DROP_INDEX.matcher(input).matches()) {
            return Commands.DROP_INDEX;
        }
        throw new IllegalArgumentException("Invalid query: " + input);
    }

    private static String getMatchingGroup(Pattern pattern, String input, int group) {
        Matcher matcher = pattern.matcher(input);
        matcher.matches();
        return matcher.group(group);
    }

    public String parseCollection() {
        switch (command) {
            case SELECT:
                return getMatchingGroup(SELECT, ql, 2);
            case INSERT:
                return getMatchingGroup(INSERT, ql, 1);
            case UPDATE:
                return getMatchingGroup(UPDATE, ql, 1);
            case DELETE:
                return getMatchingGroup(DELETE, ql, 1);
            case CREATE:
                return getMatchingGroup(CREATE, ql, 1);
            case DROP:
                return getMatchingGroup(DROP, ql, 1);
            case CREATE_INDEX:
                return getMatchingGroup(CREATE_INDEX, ql, 2);
            case DROP_INDEX:
                return getMatchingGroup(DROP_INDEX, ql, 2);
            default:
                throw new IllegalArgumentException("The query has no collection: " + ql);
        }
    }

    public String parseColumns() {
        switch (command) {
            case SELECT:
                return getMatchingGroup(SELECT, ql, 1).replace(" ", "");
            case INSERT:
                return getMatchingGroup(INSERT, ql, 1);
            case CREATE_INDEX:
                return getMatchingGroup(CREATE_INDEX, ql, 1);
            case DROP_INDEX:
                return getMatchingGroup(DROP_INDEX, ql, 1);
            default:
                throw new IllegalArgumentException("The query has no columns: " + ql);
        }
    }

    public String parseWhere() {
        switch (command) {
            case SELECT:
                return getMatchingGroup(SELECT, ql, 4);
            case UPDATE:
                return getMatchingGroup(UPDATE, ql, 8);
            case DELETE:
                return getMatchingGroup(DELETE, ql, 3);
            default:
                throw new IllegalArgumentException("The query has no where clause: " + ql);
        }
    }

    public String parseValues() {
        switch (command) {
            case INSERT:
                return getMatchingGroup(INSERT, ql, 2);
            default:
                throw new IllegalArgumentException("The query has no values clause: " + ql);
        }
    }

    public String[] parseSetColumns() {
        return parseUpdateSet(s -> s.substring(0, s.indexOf("=")));
    }

    public String[] parseSetValues() {
        return parseUpdateSet(s -> s.substring(s.indexOf("=") + 1));
    }

    private String[] parseUpdateSet(Function<String, String> mapper) {
        if (command != Commands.UPDATE) {
            throw new IllegalArgumentException("The query has no set clause: " + ql);
        }
        String setClause = getMatchingGroup(UPDATE, ql, 2).trim();
        List<String> columns = new ArrayList<>();
        Arrays.stream(setClause.split(","))
                .map(mapper)
                .map(String::trim)
                .forEach(columns::add);

        return columns.toArray(new String[]{});
    }
}
