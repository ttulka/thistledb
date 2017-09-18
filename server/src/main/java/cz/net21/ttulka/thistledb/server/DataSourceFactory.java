package cz.net21.ttulka.thistledb.server;

import java.nio.file.Path;

import cz.net21.ttulka.thistledb.db.DataSource;
import cz.net21.ttulka.thistledb.db.DataSourceFile;
import lombok.NonNull;

/**
 * Data Source Factory.
 *
 * @author ttulka
 */
final class DataSourceFactory {

    private DataSourceFactory() {
    }

    public static DataSource getDataSource(@NonNull Path dataDir, int cacheExpirationTime) {
        return new DataSourceFile(dataDir, cacheExpirationTime);
    }
}
