package cz.net21.ttulka.thistledb.server.db;

import java.nio.file.Path;

import lombok.NonNull;

/**
 * Data Source Factory.
 *
 * @author ttulka
 */
public class DataSourceFactory {

    private DataSourceFactory() {
    }

    public static DataSource getDataSource(@NonNull Path dataDir) {
        return new DataSourceFile(dataDir);
    }
}
