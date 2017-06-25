package cz.net21.ttulka.thistledb.server.db;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import lombok.extern.apachecommons.CommonsLog;
import reactor.core.publisher.Flux;

/**
 * Created by ttulka
 * <p>
 * Service to the database access.
 */
// TODO implement
@CommonsLog
public class DataSourceImpl implements DataSource {

    private Map<String, Set<JSONObject>> db = new HashMap<>();
    private Map<String, Set<String>> indexes = new HashMap<>();

    public DataSourceImpl(Path dataDir) {
        // TODO
    }

    @Override
    public boolean createCollection(String collectionName) {
        return db.putIfAbsent(collectionName, new HashSet<>()) == null;
    }

    @Override
    public boolean dropCollection(String collectionName) {
        return db.remove(collectionName) != null;
    }

    @Override
    public Flux<JSONObject> select(String collectionName, String columns, String where) {
        checkIfCollectionExists(collectionName);

        return Flux.fromIterable(db.get(collectionName));
    }

    @Override
    public void insert(String collectionName, JSONObject data) {
        checkIfCollectionExists(collectionName);

        db.get(collectionName).add(data);
    }

    @Override
    public boolean update(String collectionName, String[] columns, String[] values, String where) {
        checkIfCollectionExists(collectionName);
        return true;
    }

    @Override
    public boolean delete(String collectionName, String where) {
        checkIfCollectionExists(collectionName);
        return true;
    }

    @Override
    public boolean createIndex(String collectionName, String column) {
        checkIfCollectionExists(collectionName);

        indexes.putIfAbsent(collectionName, new HashSet<>());
        return indexes.get(collectionName).add(column);
    }

    @Override
    public boolean dropIndex(String collectionName, String column) {
        checkIfCollectionExists(collectionName);

        if (indexes.containsKey(collectionName)) {
            return indexes.get(collectionName).remove(column);
        }
        return false;
    }

    private void checkIfCollectionExists(String collectionName) {
        if (!db.containsKey(collectionName)) {
            throw new DatabaseException("Collection '" + collectionName + "' doesn't exist.");
        }
    }
}
