package cz.net21.ttulka.thistledb.server.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * Created by ttulka
 * <p>
 * Service to the database access.
 */
// TODO implement
@CommonsLog
public class DataSourceImpl implements DataSource {

    private final Path dataDir;

    private final Map<String, DbCollection> collections = new HashMap<>();

    public DataSourceImpl(@NonNull Path dataDir) {
        this.dataDir = dataDir;

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
            Files.list(dataDir).forEach(path -> {
                DbCollection collection = new DbCollection(path);
                collections.put(path.getFileName().toString(), collection);
            });
        } catch (IOException e) {
            throw new DatabaseException("Cannot read a data directory '" + dataDir.toAbsolutePath() + "': " + e.getMessage(), e);
        }
    }

    Path resolveCollection(@NonNull String collectionName) {
        return dataDir.resolve(collectionName);
    }

    DbCollection getCollection(@NonNull String collectionName) {
        return collections.get(collectionName);
    }

    boolean collectionExists(@NonNull String collectionName) {
        return collections.containsKey(collectionName);
    }

    boolean addCollection(@NonNull String collectionName, @NonNull DbCollection collection) {
        return collections.putIfAbsent(collectionName, collection) == null;
    }

    boolean removeCollection(@NonNull String collectionName) {
        return collections.remove(collectionName) != null;
    }

    @Override
    public boolean createCollection(@NonNull String collectionName) {
        if (!collectionExists(collectionName)) {
            Path path = resolveCollection(collectionName);
            try {
                Files.createFile(path);

                addCollection(collectionName, new DbCollection(path));
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
            try {
                Path path = getCollection(collectionName).getPath();
                removeCollection(collectionName);

                Files.delete(path);
                return true;

            } catch (IOException e) {
                throw new DatabaseException("Cannot drop collection '" + collectionName + "': " + e.getMessage(), e);
            }
        }
        return false;
    }

    @Override
    public Flux<String> select(@NonNull String collectionName, @NonNull String columns, String where) {
        checkIfCollectionExists(collectionName);

        DbCollection.Select select = getCollection(collectionName).select(columns, where);

        Flux<String> stream = Flux.generate(sink -> {
            String json = select.next();
            if (json != null) {
                sink.next(json);
            } else {
                sink.complete();
                select.close();
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
    public boolean delete(@NonNull String collectionName, String where) {
        checkIfCollectionExists(collectionName);

        return getCollection(collectionName).delete(where);
    }

    @Override
    public boolean delete(@NonNull String collectionName) {
        return delete(collectionName, null);
    }

    @Override
    public boolean createIndex(@NonNull String collectionName, @NonNull String column) {
        checkIfCollectionExists(collectionName);
        // TODO
        return true;
    }

    @Override
    public boolean dropIndex(@NonNull String collectionName, @NonNull String column) {
        checkIfCollectionExists(collectionName);
        // TODO
        return true;
    }

    private void checkIfCollectionExists(String collectionName) {
        if (!collectionExists(collectionName)) {
            throw new DatabaseException("Collection '" + collectionName + "' doesn't exist.");
        }
    }
}
