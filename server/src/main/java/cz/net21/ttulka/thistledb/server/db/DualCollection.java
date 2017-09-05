package cz.net21.ttulka.thistledb.server.db;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/**
 * DUAL collection.
 * <p>
 * @author ttulka
 */
public class DualCollection implements DbCollection {

    public static final String NAME = "DUAL";

    private static final DualCollection INSTANCE = new DualCollection();

    private final Random random = new Random();
    private final DateTimeFormatter dateFormatter;

    public static DualCollection getInstance() {
        return INSTANCE;
    }

    protected DualCollection() {
        dateFormatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive().append(ISO_LOCAL_DATE).appendLiteral(' ').append(ISO_LOCAL_TIME)
                .toFormatter();
    }

    @Override
    public Iterator<String> select(String element, String where) {
        if (where != null) {
            throw new IllegalStateException(NAME + " collection cannot be queried with the WHERE clause.");
        }
        if ("*".equals(element)) {
            return Collections.emptyIterator();
        }
        return Collections.singleton(getResult(element)).iterator();
    }

    private String getResult(String element) {
        if ("name".equalsIgnoreCase(element)) {
            return "{\"name\":\"" + NAME + "\"}";
        }
        if ("random".equalsIgnoreCase(element)) {
            return "{\"random\":" + random.nextInt() + "}";
        }
        if ("date".equalsIgnoreCase(element)) {
            return "{\"date\":\"" + LocalDateTime.now().format(dateFormatter) + "\"}";
        }
        return "{\"value\":" + getResultValue(element) + "}";
    }

    private String getResultValue(String element) {
        try {
            return String.valueOf(Integer.valueOf(element));
        } catch (Exception ignore) {
        }
        try {
            return String.valueOf(Double.valueOf(element));
        } catch (Exception ignore) {
        }
        if (element.equalsIgnoreCase("true")) {
            return "true";
        }
        if (element.equalsIgnoreCase("false")) {
            return "false";
        }
        return "\"" + removeQuotes(element) + "\"";
    }

    private String removeQuotes(String s) {
        if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    @Override
    public void insert(Collection<String> jsonData) {
        throw new IllegalStateException("Cannot modify DUAL collection.");
    }

    @Override
    public boolean delete(String where) {
        throw new IllegalStateException("Cannot modify DUAL collection.");
    }

    @Override
    public int update(String[] columns, String[] values, String where) {
        throw new IllegalStateException("Cannot modify DUAL collection.");
    }

    @Override
    public void cleanUp() {
    }
}
