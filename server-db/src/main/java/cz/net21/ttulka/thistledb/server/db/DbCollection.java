package cz.net21.ttulka.thistledb.server.db;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author ttulka
 */
interface DbCollection {

    Iterator<String> select(String element, String where);

    void insert(Collection<String> jsonData);

    boolean delete(String where);

    int update(String[] columns, String[] values, String where);

    boolean createIndex(String column);

    boolean dropIndex(String column);

    void cleanUp();
}
