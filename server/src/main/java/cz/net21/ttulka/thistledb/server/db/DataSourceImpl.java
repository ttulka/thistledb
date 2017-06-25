package cz.net21.ttulka.thistledb.server.db;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private Map<String, List<JSONObject>> db = new HashMap<>();

    public DataSourceImpl(Path dataDir) {
        // TODO
    }

    @Override
    public boolean createCollection(String collectionName) {
        return false;
    }

    @Override
    public boolean dropCollection(String collectionName) {
        return false;
    }

    @Override
    public Flux<JSONObject> select(String collectionName, String columns, String where) {
        return null;
    }

    @Override
    public void insert(String collectionName, JSONObject data) {

    }

    @Override
    public boolean update(String collectionName, String[] columns, String[] values, String where) {
        return false;
    }

    @Override
    public void delete(String collectionName, String where) {

    }

    @Override
    public void createIndex(String collectionName, String column) {

    }

    @Override
    public void dropIndex(String collectionName, String column) {

    }
}
