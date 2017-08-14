package cz.net21.ttulka.thistledb.server.db;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.json.JSONArray;

import cz.net21.ttulka.thistledb.tson.TSONObject;
import lombok.Data;
import lombok.NonNull;

/**
 * Created by ttulka
 * <p>
 * Represent a where clause.
 */
class Where {

    enum Operators {

        EQUAL("="),
        NOT_EQUAL("!="),
        GREATER(">"),
        LESS("<"),
        GREATER_EQUAL(">="),
        LESS_EQUAL("<="),
        LIKE("LIKE");

        private final String symbol;

        Operators(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return this.symbol;
        }
    }

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
        TSONObject jsonObject = new TSONObject(json);

        return andConditions.stream()
                .map(and -> and.matches(jsonObject))
                .allMatch(match -> match);
    }

    protected static List<Condition> parse(String where) {
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

    static List<ConditionDataPart> parseOrParts(String orPart) {
        return Arrays.stream(orPart.split(" OR "))
                .map(Where::parseDataPart)
                .collect(Collectors.toList());
    }

    static ConditionDataPart parseDataPart(String data) {
        String dataProcessed = truncateOperators(data);

        Optional<Operators> operator = Stream.of(Operators.values())
                .sorted((op1, op2) -> Integer.valueOf(op2.getSymbol().length()).compareTo(Integer.valueOf(op1.getSymbol().length())))
                .filter(op -> dataProcessed.contains(op.getSymbol()))
                .findFirst();

        if (!operator.isPresent()) {
            throw new IllegalArgumentException("Cannot parse a condition: " + data);
        }

        String[] split = dataProcessed.split(operator.get().getSymbol());

        String key = data.substring(0, split[0].length());
        String value = data.substring(data.length() - split[1].length());

        return new ConditionDataPart(key.trim(), operator.get(), removeQuotes(value.trim()));
    }

    private static String truncateOperators(String s) {
        for (Operators op : Operators.values()) {
            StringBuilder sb = new StringBuilder(s.length());
            Character quote = null;
            int i = 0;
            while (i < s.length()) {
                char ch = s.charAt(i);

                if (ch == '"' || ch == '\'') {
                    if (quote == null) {
                        quote = ch;

                    } else if (quote == ch) {
                        quote = null;
                    }
                } else if (quote != null && s.substring(i).startsWith(op.getSymbol())) {    // we are inside quotes
                    sb.append("replacementstring".substring(0, op.getSymbol().length()));
                    i += op.getSymbol().length();
                    continue;
                }
                sb.append(ch);
                i++;
            }
            s = sb.toString();
        }
        return s;
    }

    private static String removeQuotes(String s) {
        if ((s.startsWith("\"") && s.endsWith("\"")) || ((s.startsWith("'") && s.endsWith("'")))) {
            return s.substring(0, s.length() - 1).substring(1);
        }
        return s;
    }

    @Data
    static class Condition {

        private final List<ConditionDataPart> orClause;

        public boolean matches(TSONObject json) {
            return orClause.stream()
                    .map(or -> or.matches(json))
                    .anyMatch(match -> match);
        }
    }

    @Data
    static class ConditionDataPart {

        private final String key;
        private final Operators operator;
        private final String value;

        public boolean matches(TSONObject json) {
            return valueMatches(json.findByPath(key));
        }

        private boolean valueMatches(Object o) {
            if (o != null) {
                if (o instanceof JSONArray) {
                    JSONArray array = (JSONArray) o;

                    Iterator iterator = array.iterator();
                    while (iterator.hasNext()) {
                        if (valueMatches(iterator.next().toString())) {
                            return true;
                        }
                    }
                } else {
                    return valueMatches(o.toString());
                }
            }
            return false;
        }

        private boolean valueMatches(String valueToCompare) {
            try {
                return valueMatches(Double.valueOf(this.value), Double.valueOf(valueToCompare));

            } catch (NumberFormatException ignore) {
            }
            return valueMatches(this.value, valueToCompare);
        }

        private boolean valueMatches(Comparable value, Comparable valueToCompare) {
            if (valueToCompare == null) {
                return false;
            }
            switch (this.operator) {
                case EQUAL:
                    return valueToCompare.equals(value);
                case NOT_EQUAL:
                    return !valueToCompare.equals(value);
                case GREATER:
                    return valueToCompare.compareTo(value) > 0;
                case GREATER_EQUAL:
                    return valueToCompare.compareTo(value) >= 0;
                case LESS:
                    return valueToCompare.compareTo(value) < 0;
                case LESS_EQUAL:
                    return valueToCompare.compareTo(value) <= 0;
                case LIKE:
                    return like(valueToCompare.toString());
            }
            return false;
        }

        private boolean like(String valueToCompare) {
            String regex = " ".concat(this.value)
                    .replaceAll("([^\\\\]?)(\\*)", "$1(.*)")
                    .replaceAll("([^\\\\]?)(\\?)", "$1(.?)")
                    .replaceAll("([^\\\\]?)(_)", "$1(.)")
                    .substring(1);

            Matcher matcher = Pattern.compile(regex).matcher(valueToCompare);
            return matcher.matches();
        }
    }
}
