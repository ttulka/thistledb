package cz.net21.ttulka.thistledb.server.db;

import java.util.Collection;

import org.json.JSONObject;

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
     * @param condition      the condition to search
     * @return a collection of found documents
     */
    Collection<JSONObject> select(String collectionName, JSONObject condition);

    /**
     * Inserts into a collection.
     *
     * @param collectionName the collection name
     * @param data
     */
    void insert(String collectionName, JSONObject data);
}
