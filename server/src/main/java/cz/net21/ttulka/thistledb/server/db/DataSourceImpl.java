package cz.net21.ttulka.thistledb.server.db;

import java.nio.file.Path;

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

    public DataSourceImpl(Path dataDir) {
        // TODO
    }

    @Override
    public Flux<JSONObject> select(@NonNull String collectionName, @NonNull String columns, String where) {
        return null;
    }

    @Override
    public void insert(@NonNull String collectionName, @NonNull JSONObject data) {

    }
}
