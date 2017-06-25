package cz.net21.ttulka.thistledb.client;

import java.util.Arrays;

import org.json.JSONObject;

/**
 * Created by ttulka
 * <p>
 * Builder of queries.
 */
public class Query {

    private final String nativeQuery;

    private Query(String nativeQuery) {     // access only via builder
        this.nativeQuery = nativeQuery;
    }

    public static QueryBuilder builder() {
        return new QueryBuilder();
    }

    public String getNativeQuery() {
        return nativeQuery;
    }

    private static class QueryBuilder {

        private StringBuilder sb = new StringBuilder();

        private boolean command;
        private boolean select;
        private boolean setAllowed;
        private boolean whereAllowed;
        private boolean where;

        public Query build() {
            return new Query(sb.toString());
        }

        public QueryBuilder select(String from) {
            return select(new String[]{"*"}, from);
        }

        public QueryBuilder select(String[] columns, String from) {
            if (columns == null || columns.length == 0) {
                throw new IllegalArgumentException("Columns cannot be empty.");
            }
            if (from == null || from.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            select = true;

            sb.append("SELECT ");
            Arrays.stream(columns).forEach(s -> {
                if (!"SELECT".equals(sb.toString())) {
                    sb.append(", ");
                }
                sb.append(s);
            });
            sb.append(" FROM ").append(from);
            whereAllowed = true;
            return this;
        }

        public QueryBuilder where(String column, String value) {
            if (column == null || column.isEmpty()) {
                throw new IllegalArgumentException("Column name cannot be empty.");
            }
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException("Value cannot be empty.");
            }
            if (!whereAllowed) {
                throw new IllegalStateException("WHERE clause not expected.");
            }
            whereAllowed = false;
            setAllowed = false;
            where = true;
            sb.append(" WHERE ")
                    .append(column).append("=").append("'").append(value).append("'");
            return this;
        }

        public QueryBuilder and(String column, String value) {
            if (column == null || column.isEmpty()) {
                throw new IllegalArgumentException("Column name cannot be empty.");
            }
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException("Value cannot be empty.");
            }
            if (!where) {
                throw new IllegalStateException("AND clause not expected.");
            }
            sb.append(" AND ")
                    .append(column).append("=").append("'").append(value).append("'");
            return this;
        }

        public QueryBuilder or(String column, String value) {
            if (column == null || column.isEmpty()) {
                throw new IllegalArgumentException("Column name cannot be empty.");
            }
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException("Value cannot be empty.");
            }
            if (!where) {
                throw new IllegalStateException("OR clause not expected.");
            }
            sb.append(" OR ")
                    .append(column).append("=").append("'").append(value).append("'");
            return this;
        }

        public QueryBuilder insert(String into, JSONObject data) {
            if (data == null) {
                throw new IllegalArgumentException("JSON data cannot be null.");
            }
            return insert(into, data.toString());
        }

        public QueryBuilder insert(String into, String data) {
            if (into == null || into.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (data == null || data.isEmpty()) {
                throw new IllegalArgumentException("JSON data cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            sb.append("INSERT INTO ").append(into).append(" ").append(data);
            return this;
        }

        public QueryBuilder update(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            setAllowed = true;
            sb.append("UPDATE ").append(collectionName).append(" SET");
            return this;
        }

        public QueryBuilder set(String column, String value) {
            if (column == null || column.isEmpty()) {
                throw new IllegalArgumentException("Column name cannot be empty.");
            }
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException("Value cannot be empty.");
            }
            if (!setAllowed) {
                throw new IllegalStateException("SET clause not expected.");
            }
            whereAllowed = true;
            sb.append(" ").append(column).append("=").append(value);
            return this;
        }

        public QueryBuilder delete(String from) {
            if (from == null || from.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            whereAllowed = true;
            sb.append("DELETE FROM ").append(from);
            return this;
        }

        public QueryBuilder createCollection(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            sb.append("CREATE ").append(collectionName);
            return this;
        }

        public QueryBuilder dropCollection(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            sb.append("DROP ").append(collectionName);
            return this;
        }

        public QueryBuilder createIndex(String collectionName, String column) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            sb.append("CREATE INDEX ").append(column).append(" ON ").append(collectionName);
            return this;
        }

        public QueryBuilder dropIndex(String collectionName, String column) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            sb.append("DROP INDEX ").append(column).append(" ON ").append(collectionName);
            return this;
        }
    }
}
