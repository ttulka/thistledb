package cz.net21.ttulka.thistledb.server.db;

import cz.net21.ttulka.thistledb.tson.TSONObject;
import reactor.core.publisher.Flux;

/**
 * Created by ttulka
 * <p>
 * Service to the database access.
 */
public interface DataSource {

    /**
     * Creates a collection.
     *
     * @param collectionName the collection name
     * @return true if a new collection was created, otherwise false
     * @throws DatabaseException if the collection already exists
     */
    boolean createCollection(String collectionName);

    /**
     * Drops a collection.
     *
     * @param collectionName the collection name
     * @return true if the collection was dropped, otherwise false
     */
    boolean dropCollection(String collectionName);

    /**
     * Selects from a collection by a condition.
     *
     * @param collectionName the collection name
     * @param columns        the columns to select
     * @param where          the condition
     * @return a collection of found documents
     * @throws DatabaseException if the collection doesn't exist
     */
    Flux<TSONObject> select(String collectionName, String columns, String where);

    /**
     * Inserts JSON data into a collection.
     *
     * @param collectionName the collection name
     * @param data           the JSON data to insert
     * @throws DatabaseException if the collection doesn't exist
     */
    void insert(String collectionName, TSONObject data);

    /**
     * Updates a collection by a condition.
     *
     * @param collectionName the collection name
     * @param columns        the columns to update
     * @param values         the new values
     * @param where          the condition
     * @return true if the collection was updated, otherwise false
     * @throws DatabaseException if the collection doesn't exist
     */
    boolean update(String collectionName, String[] columns, String[] values, String where);

    /**
     * Deletes from a collection by a condition.
     *
     * @param collectionName the collection name
     * @param where          the condition
     * @return true if a record was deleted, otherwise false
     * @throws DatabaseException if the collection doesn't exist
     */
    boolean delete(String collectionName, String where);

    /**
     * Creates an index in a collection.
     *
     * @param collectionName the collection name
     * @param column         the column to create the index on
     * @return true if a new index was created, otherwise false
     * @throws DatabaseException if the collection doesn't exist
     */
    boolean createIndex(String collectionName, String column);

    /**
     * Drops an index from a collection.
     *
     * @param collectionName the collection name
     * @param column         the column of the index
     * @return true if the index was dropped, otherwise false
     * @throws DatabaseException if the collection doesn't exist
     */
    boolean dropIndex(String collectionName, String column);
}
