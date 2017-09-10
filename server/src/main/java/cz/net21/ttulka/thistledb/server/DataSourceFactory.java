package cz.net21.ttulka.thistledb.server;

import java.nio.file.Path;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import cz.net21.ttulka.thistledb.server.db.DataSourceFile;
import lombok.NonNull;

/**
 * Data Source Factory.
 *
 * @author ttulka
 */
final class DataSourceFactory {

    private DataSourceFactory() {
    }

    public static DataSource getDataSource(@NonNull Path dataDir) {
        return new DataSourceFile(dataDir);
    }
}
