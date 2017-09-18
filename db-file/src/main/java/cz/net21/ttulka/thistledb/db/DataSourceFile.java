package cz.net21.ttulka.thistledb.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * Service to the database access.
 *
 * @author ttulka
 */
@CommonsLog
public class DataSourceFile implements DataSource {

    protected final Path dataDir;
    protected final int cacheExpirationTime;

    protected final Map<String, DbCollection> collections = new HashMap<>();

    public DataSourceFile(@NonNull Path dataDir, int cacheExpirationTime) {
        this.dataDir = dataDir;
        this.cacheExpirationTime = cacheExpirationTime;

        if (Files.exists(dataDir)) {
            loadCollections(dataDir);
        } else {
            try {
                Files.createDirectories(dataDir);
            } catch (IOException e) {
                throw new DatabaseException("Cannot create a data directory '" + dataDir.toAbsolutePath() + "': " + e.getMessage(), e);
            }
        }
    }

    void loadCollections(Path dataDir) {
        try {
            Files.list(dataDir)
                    .filter(Files::isRegularFile)
                    .forEach(path -> collections.put(
                            path.getFileName().toString(),
                            new DbCollectionFile(path, cacheExpirationTime)));

        } catch (IOException e) {
            throw new DatabaseException("Cannot read a data directory '" + dataDir.toAbsolutePath() + "': " + e.getMessage(), e);
        }
        // add the dual table
        collections.put(DualCollection.NAME.toLowerCase(), DualCollection.getInstance());
    }

    Path resolveCollection(@NonNull String collectionName) {
        return dataDir.resolve(collectionName.toLowerCase());
    }

    DbCollection getCollection(@NonNull String collectionName) {
        return collections.get(collectionName.toLowerCase());
    }

    boolean collectionExists(@NonNull String collectionName) {
        return collections.containsKey(collectionName.toLowerCase());
    }

    boolean addCollection(@NonNull String collectionName, @NonNull DbCollection collection) {
        return collections.putIfAbsent(collectionName.toLowerCase(), collection) == null;
    }

    boolean removeCollection(@NonNull String collectionName) {
        return collections.remove(collectionName.toLowerCase()) != null;
    }

    @Override
    public boolean createCollection(@NonNull String collectionName) {
        if (!collectionExists(collectionName)) {
            Path path = resolveCollection(collectionName);
            try {
                Files.createFile(path);

                addCollection(collectionName, new DbCollectionFile(path, cacheExpirationTime));
                return true;

            } catch (IOException e) {
                throw new DatabaseException("Cannot create collection '" + collectionName + "': " + e.getMessage(), e);
            }
        }
        return false;
    }

    @Override
    public boolean dropCollection(@NonNull String collectionName) {
        if (collectionExists(collectionName)) {
            DbCollection collection = getCollection(collectionName);
            removeCollection(collectionName);

            if (collection instanceof DbCollectionFile) {
                ((DbCollectionFile) collection).drop();
            }
            return true;
        }
        return false;
    }

    @Override
    public int add(String collectionName, String element) {
        return add(collectionName, element, null);
    }

    @Override
    public int add(String collectionName, String element, String where) {
        checkIfCollectionExists(collectionName);

        return getCollection(collectionName).add(element, where);
    }

    @Override
    public int remove(String collectionName, String element) {
        return remove(collectionName, element, null);
    }

    @Override
    public int remove(String collectionName, String element, String where) {
        checkIfCollectionExists(collectionName);

        return getCollection(collectionName).remove(element, where);
    }

    @Override
    public Flux<String> select(@NonNull String collectionName, @NonNull String columns, String where) {
        checkIfCollectionExists(collectionName);

        Iterator<String> select = getCollection(collectionName).select(columns, where);

        Flux<String> stream = Flux.generate(sink -> {
            if (select.hasNext()) {
                String json = select.next();
                sink.next(json);
            } else {
                sink.complete();
            }
        });
        return stream.parallel().runOn(Schedulers.parallel()).sequential();
    }

    @Override
    public Flux<String> select(@NonNull String collectionName, @NonNull String columns) {
        return select(collectionName, columns, null);
    }

    @Override
    public void insert(@NonNull String collectionName, @NonNull String data) {
        insert(collectionName, Collections.singleton(data));
    }

    @Override
    public void insert(@NonNull String collectionName, @NonNull Collection<String> data) {
        checkIfCollectionExists(collectionName);

        getCollection(collectionName).insert(data);
    }

    @Override
    public int update(@NonNull String collectionName, @NonNull String[] columns, @NonNull String[] values, String where) {
        checkIfCollectionExists(collectionName);

        return getCollection(collectionName).update(columns, values, where);
    }

    @Override
    public int update(@NonNull String collectionName, @NonNull String[] columns, @NonNull String[] values) {
        return update(collectionName, columns, values, null);
    }

    @Override
    public int delete(@NonNull String collectionName, String where) {
        checkIfCollectionExists(collectionName);

        return getCollection(collectionName).delete(where);
    }

    @Override
    public int delete(@NonNull String collectionName) {
        return delete(collectionName, null);
    }

    @Override
    public boolean createIndex(@NonNull String collectionName, @NonNull String column) {
        checkIfCollectionExists(collectionName);
        return getCollection(collectionName).createIndex(column);
    }

    @Override
    public boolean dropIndex(@NonNull String collectionName, @NonNull String column) {
        checkIfCollectionExists(collectionName);
        return getCollection(collectionName).dropIndex(column);
    }

    private void checkIfCollectionExists(String collectionName) {
        if (!collectionExists(collectionName)) {
            throw new DatabaseException("Collection '" + collectionName + "' doesn't exist.");
        }
    }

    @Override
    public void cleanUpData() {
        collections.values().stream().forEach(DbCollection::cleanUp);
    }
}
