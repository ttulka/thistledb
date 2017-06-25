package cz.net21.ttulka.thistledb.server.db;

import org.json.JSONObject;

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
     */
    void createCollection(String collectionName);

    /**
     * Drops a collection.
     *
     * @param collectionName the collection name
     */
    void dropCollection(String collectionName);

    /**
     * Selects from a collection by a condition.
     *
     * @param collectionName the collection name
     * @param columns        the columns to select
     * @param where          the condition
     * @return a collection of found documents
     */
    Flux<JSONObject> select(String collectionName, String columns, String where);

    /**
     * Inserts JSON data into a collection.
     *
     * @param collectionName the collection name
     * @param data           the JSON data to insert
     */
    void insert(String collectionName, JSONObject data);

    /**
     * Updates a collection by a condition.
     *
     * @param collectionName the collection name
     * @param columns        the columns to update
     * @param values         the new values
     * @param where          the condition
     */
    void update(String collectionName, String[] columns, String[] values, String where);

    /**
     * Deletes from a collection by a condition.
     *
     * @param collectionName the collection name
     * @param where          the condition
     */
    void delete(String collectionName, String where);

    /**
     * Creates an index in a collection.
     *
     * @param collectionName the collection name
     * @param column         the column to create the index on
     */
    void createIndex(String collectionName, String column);

    /**
     * Drops an index from a collection.
     *
     * @param collectionName the collection name
     * @param column         the column of the index
     */
    void dropIndex(String collectionName, String column);
}
