package cz.net21.ttulka.thistledb.client;

import java.util.Arrays;

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

    public static class QueryBuilder {

        private StringBuilder sb = new StringBuilder();

        private boolean command;
        private boolean setAllowed;
        private boolean whereAllowed;
        private boolean where;
        private boolean ready;

        private QueryBuilder() {
            // private access
        }

        public Query build() {
            if (!ready) {
                throw new IllegalStateException("Query is not ready.");
            }
            return new Query(sb.toString());
        }

        public QueryBuilder selectFrom(String from) {
            return selectFrom(from, new String[]{"*"});
        }

        public QueryBuilder selectFrom(String from, String[] columns) {
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

            sb.append("SELECT ");
            Arrays.stream(columns).forEach(s -> {
                if (!"SELECT ".equals(sb.toString())) {
                    sb.append(", ");
                }
                sb.append(s);
            });
            sb.append(" FROM ").append(from);
            whereAllowed = true;
            ready = true;
            return this;
        }

        public QueryBuilder insertInto(String into, String data) {
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
            ready = true;
            sb.append("INSERT INTO ").append(into).append(" VALUES ").append(data);
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
            ready = true;
            if (!sb.toString().endsWith(" SET")) {
                sb.append(",");
            }
            sb.append(" ").append(column).append("=").append("'").append(value).append("'");
            return this;
        }

        public QueryBuilder deleteFrom(String from) {
            if (from == null || from.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            whereAllowed = true;
            ready = true;
            sb.append("DELETE FROM ").append(from);
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

        public QueryBuilder createCollection(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            ready = true;
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
            ready = true;
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
            ready = true;
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
            ready = true;
            sb.append("DROP INDEX ").append(column).append(" ON ").append(collectionName);
            return this;
        }
    }
}
