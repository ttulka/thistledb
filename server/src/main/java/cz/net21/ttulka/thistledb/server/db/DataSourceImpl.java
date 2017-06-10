package cz.net21.ttulka.thistledb.server.db;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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

    private List<JSONObject> db = new ArrayList<>();

    public DataSourceImpl(Path dataDir) {
        // TODO
    }

    @Override
    public Flux<JSONObject> select(@NonNull String collectionName, @NonNull String columns, String where) {
        return Flux.fromIterable(db);
    }

    @Override
    public void insert(@NonNull String collectionName, @NonNull JSONObject data) {
        db.add(data);
    }
}
