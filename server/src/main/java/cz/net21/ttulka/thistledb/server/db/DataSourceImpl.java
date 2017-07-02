package cz.net21.ttulka.thistledb.server.db;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import lombok.NonNull;
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
    public boolean createCollection(@NonNull String collectionName) {
        return db.putIfAbsent(collectionName, new HashSet<>()) == null;
    }

    @Override
    public boolean dropCollection(@NonNull String collectionName) {
        return db.remove(collectionName) != null;
    }

    @Override
    public Flux<JSONObject> select(@NonNull String collectionName, @NonNull String columns, String where) {
        checkIfCollectionExists(collectionName);

        return Flux.fromIterable(db.get(collectionName));
    }

    @Override
    public void insert(@NonNull String collectionName, @NonNull JSONObject data) {
        checkIfCollectionExists(collectionName);

        db.get(collectionName).add(data);
    }

    @Override
    public boolean update(@NonNull String collectionName, @NonNull String[] columns, @NonNull String[] values, String where) {
        checkIfCollectionExists(collectionName);
        return true;
    }

    @Override
    public boolean delete(@NonNull String collectionName, String where) {
        checkIfCollectionExists(collectionName);
        return db.remove(collectionName) != null;
    }

    @Override
    public boolean createIndex(@NonNull String collectionName, @NonNull String column) {
        checkIfCollectionExists(collectionName);

        indexes.putIfAbsent(collectionName, new HashSet<>());
        return indexes.get(collectionName).add(column);
    }

    @Override
    public boolean dropIndex(@NonNull String collectionName, @NonNull String column) {
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
