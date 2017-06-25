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

        public SelectQueryBuilder selectFrom(String from) {
            return selectFrom(from, new String[]{"*"});
        }

        public SelectQueryBuilder selectFrom(String from, String[] columns) {
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
            return new SelectQueryBuilder();
        }

        public InsertQueryBuilder insertInto(String into, String data) {
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
            return new InsertQueryBuilder();
        }

        public UpdateQueryBuilder update(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            setAllowed = true;
            sb.append("UPDATE ").append(collectionName).append(" SET");
            return new UpdateQueryBuilder();
        }

        public DeleteQueryBuilder deleteFrom(String from) {
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
            return new DeleteQueryBuilder();
        }

        public CreateQueryBuilder createCollection(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            ready = true;
            sb.append("CREATE ").append(collectionName);
            return new CreateQueryBuilder();
        }

        public DropQueryBuilder dropCollection(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            ready = true;
            sb.append("DROP ").append(collectionName);
            return new DropQueryBuilder();
        }

        public CreateIndexQueryBuilder createIndex(String collectionName, String column) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            ready = true;
            sb.append("CREATE INDEX ").append(column).append(" ON ").append(collectionName);
            return new CreateIndexQueryBuilder();
        }

        public DropIndexQueryBuilder dropIndex(String collectionName, String column) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            if (command) {
                throw new IllegalStateException("Query has already a command.");
            }
            command = true;
            ready = true;
            sb.append("DROP INDEX ").append(column).append(" ON ").append(collectionName);
            return new DropIndexQueryBuilder();
        }

        public abstract class BuildableBuilder {

            public Query build() {
                if (!ready) {
                    throw new IllegalStateException("Query is not ready.");
                }
                return new Query(sb.toString());
            }
        }

        public abstract class ConditionalQueryBuilder extends BuildableBuilder {

            public ConditionalQueryBuilder where(String column, String value) {
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

            public ConditionalQueryBuilder and(String column, String value) {
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

            public ConditionalQueryBuilder or(String column, String value) {
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
        }

        public class SelectQueryBuilder extends ConditionalQueryBuilder {
        }

        public class InsertQueryBuilder extends BuildableBuilder {
        }

        public class UpdateQueryBuilder extends ConditionalQueryBuilder {

            public UpdateQueryBuilder set(String column, String value) {
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
        }

        public class DeleteQueryBuilder extends ConditionalQueryBuilder {
        }

        public class CreateQueryBuilder extends BuildableBuilder {
        }

        public class DropQueryBuilder extends BuildableBuilder {
        }

        public class CreateIndexQueryBuilder extends BuildableBuilder {
        }

        public class DropIndexQueryBuilder extends BuildableBuilder {
        }
    }
}
