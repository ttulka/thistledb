package cz.net21.ttulka.thistledb.server.db;

import java.nio.file.Path;
import java.util.Collection;

import org.json.JSONObject;

import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ttulka
 * <p>
 * Service to the database access.
 */
// TODO implement
@CommonsLog
public class DataSourceImpl implements DataSource {

    public DataSourceImpl(Path dataDir) {
    }

    @Override
    public Collection<JSONObject> select(@NonNull String collectionName, JSONObject condition) {
        return null;
    }

    @Override
    public void insert(@NonNull String collectionName, @NonNull JSONObject data) {

    }
}
