package cz.net21.ttulka.thistledb.server.db;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import cz.net21.ttulka.thistledb.tson.TSONObject;
import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;
import reactor.core.publisher.Flux;

/**
 * Created by ttulka
 * <p>
 * Mock Service to the database access.
 */
@CommonsLog
public class DataSourceMockImpl implements DataSource {

    private Map<String, Set<TSONObject>> db = new LinkedHashMap<>();
    private Map<String, Set<String>> indexes = new LinkedHashMap<>();

    public DataSourceMockImpl(Path dataDir) {
        // ignore
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
    public Flux<TSONObject> select(@NonNull String collectionName, @NonNull String columns, String where) {
        checkIfCollectionExists(collectionName);

        return Flux.fromIterable(db.get(collectionName));
    }

    @Override
    public void insert(@NonNull String collectionName, @NonNull TSONObject data) {
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
