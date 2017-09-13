package cz.net21.ttulka.thistledb.db;

import java.nio.file.Path;
import java.util.Set;

/**
 * @author ttulka
 */
interface Indexing {

    Path getPath();

    boolean exists(String index);

    Set<Long> positions(String index, String value);

    void insert(String index, Object value, long position);

    void delete(String index, Object value, long position);

    boolean create(String index);

    void drop(String index);

    void dropAll();

    void dropOnlyData();

    void cleanUp(String index);

    void cleanUp();

}
