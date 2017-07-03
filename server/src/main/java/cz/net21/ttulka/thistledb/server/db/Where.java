package cz.net21.ttulka.thistledb.server.db;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;

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

    private final List<Condition> andConditions;

    public static Where create(String where) {
        return where != null ? new Where(where) : EMPTY;
    }

    private Where(@NonNull String where) {
        this.andConditions = parse(where);
    }

    private Where() {
        andConditions = null;
    }

    public boolean matches(@NonNull String json) {
        if (json == null) {
            return false;
        }
        JSONObject jsonObject = new JSONObject(json);

        return andConditions.stream()
                .map(and -> and.matches(jsonObject))
                .allMatch(match -> match);
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

        public boolean matches(JSONObject json) {
            return orClause.stream()
                    .map(or -> or.matches(json))
                    .anyMatch(match -> match);
        }
    }

    @Data
    static class DataPart {

        private final String key;
        private final String value;

        public boolean matches(JSONObject json) {
            return valueMatches(findValue(json));
        }

        Object findValue(JSONObject json) {
            Object toReturn = null;

            for (String keyPart : key.split("\\.")) {
                if (json == null || !json.keySet().contains(keyPart)) {
                    return null;
                }
                toReturn = json.get(keyPart);

                if (toReturn instanceof JSONObject) {
                    json = (JSONObject)toReturn;
                } else {
                    json = null;
                }
            }
            return toReturn;
        }

        boolean valueMatches(Object o) {
            if (o != null) {
                if (o instanceof JSONArray) {
                    JSONArray array = (JSONArray) o;

                    Iterator iterator = array.iterator();
                    while (iterator.hasNext()) {
                        if (iterator.next().toString().equals(value)) {
                            return true;
                        }
                    }
                } else {
                    return o.toString().equals(value);
                }
            }
            return false;
        }
    }
}
