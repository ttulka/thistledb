package cz.net21.ttulka.thistledb.server.db;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.NonNull;

/**
 * Created by ttulka
 * <p>
 * Represent a where clause.
 */
class Where {

    public static final Where EMPTY = new Where() {
        public boolean matches(String json) {
            return true;
        }
    };

    private final List<Condition> conditions;

    public static Where create(String where) {
        return where != null ? new Where(where) : EMPTY;
    }

    private Where(@NonNull String where) {
        this.conditions = parse(where);
    }

    private Where() {
        conditions = null;
    }

    public boolean matches(@NonNull String json) {
        if (json == null) {
            return false;
        }
        // TODO
        return true;
    }

    private static List<Condition> parse(String where) {
        where = upperCaseIgnoreAndOrInQuotes(where, "AND");
        where = upperCaseIgnoreAndOrInQuotes(where, "OR");

        return parseAndParts(where);
    }

    static String upperCaseIgnoreAndOrInQuotes(String where, String operator) {
        return where.replaceAll("([^\"](.*))\\s+(?i)" + operator + "\\s+((.*)[^\"])", "$1 " + operator + " $3");
    }

    static List<Condition> parseAndParts(String where) {
        return Arrays.stream(where.split(" AND "))
                .map(Where::parseOrParts)
                .map(Condition::new)
                .collect(Collectors.toList());
    }

    static List<DataPart> parseOrParts(String orPart) {
        return Arrays.stream(orPart.split(" OR "))
                .map(Where::parseDataPart)
                .collect(Collectors.toList());
    }

    static DataPart parseDataPart(String data) {
        if (data.indexOf("=") == -1) {
            throw new IllegalArgumentException("Cannot parse a condition: " + data);
        }
        String[] split = data.split("=");

        return new DataPart(split[0].trim(), removeQuotes(split[1].trim()));
    }

    private static String removeQuotes(String s) {
        if ((s.startsWith("\"") && s.endsWith("\"")) || ((s.startsWith("'") && s.endsWith("'")))) {
            return s.substring(0, s.length() - 1).substring(1);
        }
        return s;
    }

    @Data
    static class Condition {
        private final List<DataPart> orClause;
    }

    @Data
    static class DataPart {
        private final String key;
        private final String value;
    }
}
