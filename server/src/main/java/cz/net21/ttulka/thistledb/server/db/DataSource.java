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
     * Selects from a collection by the condition.
     *
     * @param collectionName the collection name
     * @param columns        the columns to select
     * @param where          the condition
     * @return a collection of found documents
     */
    Flux<JSONObject> select(String collectionName, String columns, String where);

    /**
     * Inserts into a collection.
     *
     * @param collectionName the collection name
     * @param data
     */
    void insert(String collectionName, JSONObject data);
}
