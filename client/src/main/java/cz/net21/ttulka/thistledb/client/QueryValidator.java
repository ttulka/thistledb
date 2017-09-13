package cz.net21.ttulka.thistledb.client;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;

/**
 * Query validator.
 *
 * @author ttulka
 */
public class QueryValidator {

    private static final String COLLECTION = "[\\w\\d_]+";

    private static final String JSON = "\\{.*}";
    private static final String JSON_ELEMENT = "[\\w\\d._\\-$]+";
    private static final String JSON_VALUE = "((null)|(\".+\")|('.+')|(true)|(false)|(\\d+)|([\\d]*[.]?[\\d]+))";

    private static final String WHERE = "(" + JSON_ELEMENT + "\\s*(=|!=|<|<=|>|>=|LIKE)\\s*" + JSON_VALUE + ")+";
    private static final String WHERE_COMPOSITED = "(" + WHERE + ")(\\s+(AND|OR)\\s+(" + WHERE + "))*";

    static final Pattern SELECT = compile("SELECT\\s+(\\*|" + JSON_ELEMENT + ")\\s+FROM\\s+(" + COLLECTION + ")(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern INSERT = compile("INSERT\\s+INTO\\s+(" + COLLECTION + ")\\s+VALUES\\s+(" + JSON + ")", CASE_INSENSITIVE);
    static final Pattern UPDATE = compile("UPDATE\\s+(" + COLLECTION + ")\\s+SET\\s+((" + JSON_ELEMENT + "\\s*=\\s*" + JSON_VALUE + ")(\\s*,\\s*" + JSON_ELEMENT + "\\s*=\\s*" + JSON_VALUE + ")*)", CASE_INSENSITIVE);
    static final Pattern UPDATE_WHERE = compile("UPDATE\\s+(" + COLLECTION + ")\\s+SET\\s+((" + JSON_ELEMENT + "\\s*=\\s*" + JSON_VALUE + ")(\\s*,\\s*" + JSON_ELEMENT + "\\s*=\\s*" + JSON_VALUE + ")*)\\s+WHERE\\s+(" + WHERE_COMPOSITED + ")", CASE_INSENSITIVE);
    static final Pattern DELETE = compile("DELETE\\s+FROM\\s+(" + COLLECTION + ")(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern CREATE = compile("CREATE\\s+(?!.*INDEX)(" + COLLECTION + ")", CASE_INSENSITIVE);
    static final Pattern DROP = compile("DROP\\s+((?!.*INDEX)" + COLLECTION + ")", CASE_INSENSITIVE);
    static final Pattern ADD = compile("ALTER\\s+(" + COLLECTION + ")\\s+ADD((\\s+((?!.WHERE)(" + JSON_ELEMENT + "))+)\\s*)(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern REMOVE = compile("ALTER\\s+(" + COLLECTION + ")\\s+REMOVE((\\s+((?!.WHERE)(" + JSON_ELEMENT + "))+)\\s*)(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern CREATE_INDEX = compile("CREATE\\s+INDEX\\s+(" + JSON_ELEMENT + ")\\s+ON\\s+(" + COLLECTION + ")", CASE_INSENSITIVE);
    static final Pattern DROP_INDEX = compile("DROP\\s+INDEX\\s+(" + JSON_ELEMENT + ")\\s+ON\\s+(" + COLLECTION + ")", CASE_INSENSITIVE);

    private static final List<String> OPERATORS = Arrays.asList("=", "!=", ">", "<", ">=", "<=", "LIKE");

    /**
     * Validates a query.
     *
     * @param query the query
     * @return true if the query is valid, otherwise false
     */
    public static boolean validate(String query) {
        if (query == null || query.isEmpty()) {
            return false;
        }
        query = cleanQuery(query);

        if (!SELECT.matcher(query).matches()
            && !INSERT.matcher(query).matches()
            && !UPDATE.matcher(query).matches()
            && !UPDATE_WHERE.matcher(query).matches()
            && !DELETE.matcher(query).matches()
            && !CREATE.matcher(query).matches()
            && !DROP.matcher(query).matches()
            && !ADD.matcher(query).matches()
            && !REMOVE.matcher(query).matches()
            && !CREATE_INDEX.matcher(query).matches()
            && !DROP_INDEX.matcher(query).matches()) {
            return false;
        }

        if (!validateInsert(query)) {
            return false;
        }
        if (!validateWhere(query)) {
            return false;
        }
        return true;
    }

    private static String cleanQuery(String ql) {
        ql = ql.trim();
        if (ql.endsWith(";")) {
            ql = ql.substring(0, ql.length() - 1);
        }
        return ql;
    }

    private static boolean validateInsert(String query) {
        Matcher matcher = INSERT.matcher(query);
        if (matcher.matches()) {
            String json = matcher.group(2);
            return validateListOfJsons(json);
        }
        return true;
    }

    private static boolean validateWhere(String query) {
        if (SELECT.matcher(query).matches()) {
            return validateWhereClause(getMatchingGroup(SELECT, query, 4));
        }
        if (DELETE.matcher(query).matches()) {
            return validateWhereClause(getMatchingGroup(DELETE, query, 3));
        }
        if (ADD.matcher(query).matches()) {
            return validateWhereClause(getMatchingGroup(ADD, query, 7));
        }
        if (REMOVE.matcher(query).matches()) {
            return validateWhereClause(getMatchingGroup(REMOVE, query, 7));
        }
        if (UPDATE_WHERE.matcher(query).matches()) {
            return validateWhereClause(getMatchingGroup(UPDATE_WHERE, query, 21));
        }
        return true;
    }

    private static String getMatchingGroup(Pattern pattern, String input, int group) {
        try {
            Matcher matcher = pattern.matcher(input);
            matcher.matches();
            return matcher.group(group);

        } catch (Exception ignore) {
            return null;
        }
    }

    private static boolean validateWhereClause(String where) {
        if (where == null) {
            return true;
        }
        where = upperCaseIgnoreAndOrInQuotes(where, "AND");
        where = upperCaseIgnoreAndOrInQuotes(where, "OR");

        return parseAndParts(where);
    }

    private static String upperCaseIgnoreAndOrInQuotes(String where, String operator) {
        return where.replaceAll("([^\"](.*))\\s+(?i)" + operator + "\\s+((.*)[^\"])", "$1 " + operator + " $3");
    }

    private static boolean parseAndParts(String where) {
        return Arrays.stream(where.split(" AND "))
                .map(QueryValidator::parseOrParts)
                .allMatch(valid -> valid);
    }

    static boolean parseOrParts(String orPart) {
        return Arrays.stream(orPart.split(" OR "))
                .map(QueryValidator::parseDataPart)
                .allMatch(valid -> valid);
    }

    static boolean parseDataPart(String data) {
        String dataProcessed = truncateOperators(data).toUpperCase();

        Optional<String> operator = OPERATORS.stream()
                .sorted((op1, op2) -> Integer.valueOf(op2.length()).compareTo(Integer.valueOf(op1.length())))
                .filter(op -> dataProcessed.contains(op.toUpperCase()))
                .findFirst();

        if (!operator.isPresent()) {
            return false;
        }

        String[] split = dataProcessed.split(operator.get());
        return split.length == 2;
    }

    private static String truncateOperators(String s) {
        for (String op : OPERATORS) {
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
                } else if (quote != null && s.substring(i).startsWith(op)) {    // we are inside quotes
                    sb.append("replacementstring".substring(0, op.length()));
                    i += op.length();
                    continue;
                }
                sb.append(ch);
                i++;
            }
            s = sb.toString();
        }
        return s;
    }

    static boolean validateJson(String json) {
        if (json == null || json.isEmpty()) {
            return false;
        }
        try {
            new JSONTokener(json).tokenize();

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    static boolean validateListOfJsons(String listOfJsons) {
        if (listOfJsons == null || listOfJsons.isEmpty()) {
            return false;
        }
        try {
            new JSONTokener(listOfJsons).tokenizeList();

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Simple JSON Tokenizer
     */
    static class JSONTokener {

        private final String json;

        private JSONTokener(String json) {
            this.json = json.replaceAll("\\s", "");
        }

        /**
         * Tokenize the JSON input.
         *
         * @throws IllegalStateException if the input doesn't match JSON
         */
        public void tokenize() {
            String jsonObject = tokenObject(json);

            if (!jsonObject.isEmpty()) {
                throw new IllegalStateException("Expected EOF in " + getAround(jsonObject) + ".");
            }
        }

        public void tokenizeList() {
            String jsonObject = json;
            do {
                jsonObject = tokenObject(jsonObject);
                jsonObject = peekLiteral(",", jsonObject);

            } while (!jsonObject.isEmpty());
        }

        String tokenObject(String token) {
            if (!token.startsWith("{")) {
                throw new IllegalStateException("JSON Object must start with '{'.");
            }
            token = token.substring(1);

            boolean first = true;

            while (!token.startsWith("}")) {
                if (!first) {
                    token = tokenLiteral(",", token);
                }
                token = tokenString(token);
                token = tokenLiteral(":", token);
                token = tokenValue(token);

                first = false;
            }
            return token.substring(1);
        }

        String tokenString(String token) {
            if (!token.startsWith("'") && !token.startsWith("\"")) {
                throw new IllegalStateException("JSON String must start with ' or \".");
            }
            char startWith = token.charAt(0);

            int index = 1;
            char ch;
            do {
                ch = token.charAt(index++);

                if (ch == '\\') {   // escape
                    ch = token.charAt(index++);
                    if (ch == startWith) {
                        ch = '\0';
                    }
                }
            } while (ch != startWith);

            return token.substring(index);
        }

        String tokenArray(String token) {
            if (!token.startsWith("[")) {
                throw new IllegalStateException("JSON Array must start with '['.");
            }
            token = token.substring(1);

            boolean first = true;

            while (!token.startsWith("]")) {
                if (!first) {
                    token = tokenLiteral(",", token);
                }
                token = tokenValue(token);

                first = false;
            }
            return token.substring(1);
        }

        String tokenNumber(String token) {
            token = peekLiteral("-", token);
            token = token.toLowerCase();

            boolean hasDigit = false;
            boolean canBeSign = false;
            boolean canBeSigned = true;
            boolean canBeExp = true;

            int index = 0;
            char ch;
            boolean isNumber = false;

            while (true) {
                ch = token.charAt(index++);

                if (canBeSign && (ch == '+' || ch == '-')) {
                    canBeSign = false;
                    continue;
                }

                if (isDigit(ch)) {
                    hasDigit = true;
                    isNumber = true;
                    continue;
                }
                if (hasDigit && canBeSigned && ch == '.') {
                    hasDigit = false;
                    canBeSigned = false;
                    continue;
                }
                if (hasDigit && canBeExp && ch == 'e') {
                    hasDigit = false;
                    canBeSign = true;
                    canBeExp = false;
                    canBeSigned = false;
                    continue;
                }
                break;
            }

            if (!isNumber) {
                throw new IllegalStateException("Not a number literal in '" + getAround(token) + "'.");
            }
            return token.substring(index - 1);
        }

        private boolean isDigit(char ch) {
            return Pattern.matches("[0-9]", ch + "");
        }

        String tokenValue(String token) {
            try {
                return tokenString(token);

            } catch (Exception ignore) {
            }
            try {
                return tokenNumber(token);

            } catch (Exception ignore) {
            }
            try {
                return tokenObject(token);

            } catch (Exception ignore) {
            }
            try {
                return tokenArray(token);

            } catch (Exception ignore) {
            }
            try {
                return tokenLiteral("true", token);

            } catch (Exception ignore) {
            }
            try {
                return tokenLiteral("false", token);

            } catch (Exception ignore) {
            }
            try {
                return tokenLiteral("null", token);

            } catch (Exception ignore) {
            }
            throw new IllegalStateException("Expected a value in '" + getAround(token) + "'.");
        }

        String tokenLiteral(String literal, String token) {
            if (!token.startsWith(literal)) {
                throw new IllegalStateException("Expected '" + literal + "' in '" + getAround(token) + "'.");
            }
            return token.substring(literal.length());
        }

        String peekLiteral(String literal, String token) {
            try {
                return tokenLiteral(literal, token);
            } catch (Exception ignore) {
            }
            return token;
        }

        private String getAround(String json) {
            return json.substring(0, Math.min(10, json.length()));
        }
    }
}
