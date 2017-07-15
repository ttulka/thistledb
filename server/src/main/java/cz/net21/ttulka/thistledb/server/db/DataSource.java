package cz.net21.ttulka.thistledb.server.db;

import java.util.Collection;

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
     * @param element        the element to select
     * @param where          the condition
     * @return a collection of found documents
     * @throws DatabaseException if the collection doesn't exist
     */
    Flux<String> select(String collectionName, String element, String where);

    /**
     * Selects from a collection. Convenient method.
     *
     * @param collectionName the collection name
     * @param element        the element to select
     * @return a collection of found documents
     * @throws DatabaseException if the collection doesn't exist
     */
    Flux<String> select(String collectionName, String element);

    /**
     * Inserts JSON data into a collection.
     *
     * @param collectionName the collection name
     * @param data           the JSON data to insert
     * @throws DatabaseException if the collection doesn't exist
     */
    void insert(String collectionName, String data);

    /**
     * Inserts JSON data into a collection.
     *
     * @param collectionName the collection name
     * @param data           the JSON data to insert
     * @throws DatabaseException if the collection doesn't exist
     */
    void insert(String collectionName, Collection<String> data);

    /**
     * Updates a collection by a condition.
     *
     * @param collectionName the collection name
     * @param columns        the columns to update
     * @param values         the new values
     * @param where          the condition
     * @return count of updated documents
     * @throws DatabaseException if the collection doesn't exist
     */
    int update(String collectionName, String[] columns, String[] values, String where);

    /**
     * Updates a collection. Convenient method.
     *
     * @param collectionName the collection name
     * @param columns        the columns to update
     * @param values         the new values
     * @return count of updated documents
     * @throws DatabaseException if the collection doesn't exist
     */
    int update(String collectionName, String[] columns, String[] values);

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
     * Deletes from a collection. Convenient method.
     *
     * @param collectionName the collection name
     * @return true if a record was deleted, otherwise false
     * @throws DatabaseException if the collection doesn't exist
     */
    boolean delete(String collectionName);

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
