package cz.net21.ttulka.thistledb.client;

import java.util.Arrays;

/**
 * Builder of queries.
 *
 * @author ttulka
 */
public class Query {

    private final String nativeQuery;

    private Query(String nativeQuery) {     // access only via builder
        this.nativeQuery = nativeQuery;
    }

    /**
     * Returns a query builder.
     *
     * @return the query builder
     */
    public static QueryBuilder builder() {
        return new QueryBuilder();
    }

    /**
     * Returns the string representation of the query.
     *
     * @return the native query
     */
    public String getNativeQuery() {
        return nativeQuery;
    }

    /**
     * Query Builder.
     */
    public static class QueryBuilder {

        private StringBuilder sb = new StringBuilder();

        private boolean whereAllowedA;
        private boolean whereA;
        private boolean ready;

        private QueryBuilder() {
            // private access
        }

        /**
         * Builds a select query for all the columns.
         *
         * @param from the collection to select from
         * @return the select query builder
         */
        public SelectQueryBuilder selectFrom(String from) {
            return selectFrom(from, new String[]{"*"});
        }

        /**
         * Builds a select query for columns.
         *
         * @param from the collection to select from
         * @return the select query builder
         */
        public SelectQueryBuilder selectFrom(String from, String[] columns) {
            if (columns == null || columns.length == 0) {
                throw new IllegalArgumentException("Columns cannot be empty.");
            }
            if (from == null || from.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            sb.append("SELECT ");
            Arrays.stream(columns).forEach(s -> {
                if (!"SELECT ".equals(sb.toString())) {
                    sb.append(", ");
                }
                sb.append(s);
            });
            sb.append(" FROM ").append(from);
            ready = true;
            return new SelectQueryBuilder();
        }

        /**
         * Builds an insert query.
         *
         * @param into the collection to insert into
         * @return the insert query builder
         */
        public InsertQueryBuilder insertInto(String into) {
            if (into == null || into.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            sb.append("INSERT INTO ").append(into);
            return new InsertQueryBuilder();
        }

        /**
         * Builds an update query.
         *
         * @param collectionName the collection to update
         * @return the update query builder
         */
        public UpdateQueryBuilder update(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            sb.append("UPDATE ").append(collectionName).append(" SET");
            return new UpdateQueryBuilder();
        }

        /**
         * Builds a delete query.
         *
         * @param from the collection to delete from
         * @return the delete query builder
         */
        public DeleteQueryBuilder deleteFrom(String from) {
            if (from == null || from.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            ready = true;
            sb.append("DELETE FROM ").append(from);
            return new DeleteQueryBuilder();
        }

        /**
         * Builds a create collection query.
         *
         * @param collectionName the collection to be created
         * @return the create collection query builder
         */
        public CreateQueryBuilder createCollection(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            ready = true;
            sb.append("CREATE ").append(collectionName);
            return new CreateQueryBuilder();
        }

        /**
         * Builds a drop collection query.
         *
         * @param collectionName the collection to be dropped
         * @return the drop collection query builder
         */
        public DropQueryBuilder dropCollection(String collectionName) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            ready = true;
            sb.append("DROP ").append(collectionName);
            return new DropQueryBuilder();
        }

        /**
         * Builds a create insert query.
         *
         * @param collectionName the collection to be created on
         * @param column         the column to be indexed
         * @return the create insert query builder
         */
        public CreateIndexQueryBuilder createIndex(String collectionName, String column) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            ready = true;
            sb.append("CREATE INDEX ").append(column).append(" ON ").append(collectionName);
            return new CreateIndexQueryBuilder();
        }

        /**
         * Builds a drop insert query.
         *
         * @param collectionName the collection to be dropped on
         * @param column         the column of insert to be dropped
         * @return the drop insert query builder
         */
        public DropIndexQueryBuilder dropIndex(String collectionName, String column) {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name cannot be empty.");
            }
            ready = true;
            sb.append("DROP INDEX ").append(column).append(" ON ").append(collectionName);
            return new DropIndexQueryBuilder();
        }

        /**
         * Buildable Query Builder.
         */
        public abstract class BuildableBuilder {

            /**
             * Builds a query object.
             *
             * @return the query
             */
            public Query build() {
                if (!ready) {
                    throw new IllegalStateException("Query is not ready.");
                }
                return new Query(sb.toString());
            }
        }

        /**
         * Conditional Query Builder.
         */
        public abstract class ConditionalQueryBuilder extends BuildableBuilder {

            protected boolean whereAllowed = true;
            protected boolean where;

            /**
             * Builds a WHERE clause.
             *
             * @param column the column
             * @param value  the value of the column
             * @return the conditional builder
             */
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
                where = true;
                sb.append(" WHERE ")
                        .append(column).append("=").append("'").append(value).append("'");
                return this;
            }

            /**
             * Builds an AND part in a WHERE clause.
             *
             * @param column the column
             * @param value  the value of the column
             * @return the conditional builder
             */
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

            /**
             * Builds an OR part in a WHERE clause.
             *
             * @param column the column
             * @param value  the value of the column
             * @return the conditional builder
             */
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

        /**
         * Select Query Builder.
         */
        public class SelectQueryBuilder extends ConditionalQueryBuilder {
        }

        /**
         * Insert Query Builder.
         */
        public class InsertQueryBuilder extends BuildableBuilder {

            private boolean hasValues = false;

            public InsertQueryBuilder values(String data) {
                if (data == null || data.isEmpty()) {
                    throw new IllegalArgumentException("JSON data cannot be empty.");
                }
                if (!hasValues) {
                    sb.append(" VALUES ");
                } else {
                    sb.append(",");
                }
                sb.append(data);
                hasValues = true;
                ready = true;
                return this;
            }

            @Override
            public Query build() {
                if (!hasValues) {
                    throw new IllegalStateException("INSERT clause must contain VALUES.");
                }
                return super.build();
            }
        }

        /**
         * Update Query Builder.
         */
        public class UpdateQueryBuilder extends ConditionalQueryBuilder {

            public UpdateQueryBuilder set(String column, String value) {
                if (column == null || column.isEmpty()) {
                    throw new IllegalArgumentException("Column name cannot be empty.");
                }
                if (value == null || value.isEmpty()) {
                    throw new IllegalArgumentException("Value cannot be empty.");
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

        /**
         * Delete Query Builder.
         */
        public class DeleteQueryBuilder extends ConditionalQueryBuilder {
        }

        /**
         * Create collection Query Builder.
         */
        public class CreateQueryBuilder extends BuildableBuilder {
        }

        /**
         * Drop collection Query Builder.
         */
        public class DropQueryBuilder extends BuildableBuilder {
        }

        /**
         * Create insert Query Builder.
         */
        public class CreateIndexQueryBuilder extends BuildableBuilder {
        }

        /**
         * Drop insert Query Builder.
         */
        public class DropIndexQueryBuilder extends BuildableBuilder {
        }
    }
}
