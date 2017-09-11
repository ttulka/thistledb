package cz.net21.ttulka.thistledb.server.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cz.net21.ttulka.thistledb.tson.TSONObject;

/**
 * Iterates through all the columns with simple values (String, Number, Boolean).
 *
 * @author ttulka
 */
class ColumnsIterator implements Iterator<String> {

    private final List<String> columns = new ArrayList<>();
    private final Iterator<String> columnsIterator;

    public ColumnsIterator(TSONObject tson) {
        addColumns(tson, null);
        columnsIterator = columns.iterator();
    }

    private void addColumns(TSONObject tson, String parentKey) {
        tson.keySet().stream().forEach(key -> {
            String wholeKey = parentKey != null ? parentKey + "." + key : key;

            Object column = tson.get(key);

            if (column instanceof TSONObject) {
                addColumns((TSONObject) column, wholeKey);
            } else {
                columns.add(wholeKey);
            }
        });
    }

    @Override
    public boolean hasNext() {
        return columnsIterator.hasNext();
    }

    @Override
    public String next() {
        return columnsIterator.next();
    }
}
