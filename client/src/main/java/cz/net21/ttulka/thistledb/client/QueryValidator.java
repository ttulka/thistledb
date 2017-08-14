package cz.net21.ttulka.thistledb.client;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;

/**
 * Created by ttulka
 * <p>
 * Query validator.
 */
public class QueryValidator {

    private static final String COLLECTION = "[\\w\\d]+";

    private static final String JSON = "\\{.*}";    // TODO

    private static final String JSON_PATH = "[\\w\\d._\\-$]+";    // TODO

    private static final String WHERE = "(" + JSON_PATH + "\\s*(=|!=|<|<=|>|>=|LIKE)\\s*((['\"](.+)['\"])|(\\d+)))+";
    private static final String WHERE_COMPOSITED = "(" + WHERE + ")(\\s+(AND|OR)\\s+(" + WHERE + "))*";

    static final Pattern SELECT = compile("SELECT\\s+(\\*|" + JSON_PATH + ")\\s+FROM\\s+(" + COLLECTION + ")(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern INSERT = compile("INSERT\\s+INTO\\s+(" + COLLECTION + ")\\s+VALUES\\s+(" + JSON + ")", CASE_INSENSITIVE);
    static final Pattern UPDATE = compile("UPDATE\\s+(" + COLLECTION + ")\\s+SET((\\s+((?!.WHERE).)+)\\s*=\\s*(((?!.WHERE).)+))(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern DELETE = compile("DELETE\\s+FROM\\s+(" + COLLECTION + ")(\\s+WHERE\\s+(" + WHERE_COMPOSITED + "))?", CASE_INSENSITIVE);
    static final Pattern CREATE = compile("CREATE\\s+(?!.*INDEX)(" + COLLECTION + ")", CASE_INSENSITIVE);
    static final Pattern DROP = compile("DROP\\s+((?!.*INDEX)" + COLLECTION + ")", CASE_INSENSITIVE);
    static final Pattern CREATE_INDEX = compile("CREATE\\s+INDEX\\s+(" + JSON_PATH + ")\\s+ON\\s+(" + COLLECTION + ")", CASE_INSENSITIVE);
    static final Pattern DROP_INDEX = compile("DROP\\s+INDEX\\s+(" + JSON_PATH + ")\\s+ON\\s+(" + COLLECTION + ")", CASE_INSENSITIVE);

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
            && !DELETE.matcher(query).matches()
            && !CREATE.matcher(query).matches()
            && !DROP.matcher(query).matches()
            && !CREATE_INDEX.matcher(query).matches()
            && !DROP_INDEX.matcher(query).matches()) {
            return false;
        }

        if (!validateInsert(query)) {
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
