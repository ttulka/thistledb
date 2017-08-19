package cz.net21.ttulka.thistledb.server.db;

import java.util.Collection;
import java.util.Iterator;

import lombok.NonNull;

/**
 * Created by ttulka
 */
interface DbCollection {

    Iterator<String> select(String element, String where);

    void insert(@NonNull Collection<String> jsonData);

    boolean delete(String where);

    int update(String[] columns, String[] values, String where);

    void cleanUp();
}
