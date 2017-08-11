package cz.net21.ttulka.thistledb.server.db;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.json.JSONArray;
import org.json.JSONObject;

import cz.net21.ttulka.thistledb.tson.TSONObject;
import lombok.NonNull;

/**
 * Created by ttulka
 */
// TODO
public class DbCollection {

    static final char RECORD_SEPARATOR = '\1';
    static final char RECORD_DELETED = '\2';

    private final Path path;

    public DbCollection(@NonNull Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public Select select(@NonNull String element, String where) {
        lock.readLock().lock();
        try {
            return new Select(element, where);

        } catch (IOException e) {
            throw new DatabaseException("Cannot work with a collection: " + e.getMessage(), e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void insert(@NonNull Collection<String> jsonData) {
        lock.writeLock().lock();
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.APPEND)) {
            for (String json : jsonData) {
                writer.write(serialize(json));
                writer.write(RECORD_SEPARATOR);
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot insert into a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void insert(String json) {
        insert(Collections.singleton(json));
    }

    public boolean delete(String where) {
        lock.writeLock().lock();
        try {
            return new Delete(where).delete();

        } catch (IOException e) {
            throw new DatabaseException("Cannot delete from a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int update(String[] columns, String[] values, String where) {
        lock.writeLock().lock();
        try {
            return new Update(columns, values, where).update();

        } catch (IOException e) {
            throw new DatabaseException("Cannot update a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    String serialize(String tson) {
        return tson;
    }

    String deserialize(String tson) {
        return tson;
    }

    abstract class DbAccess implements AutoCloseable {

        static final int BUFFER_SIZE = 1024;

        protected final RandomAccessFile file;
        protected final FileChannel channel;

        protected DbAccess() throws IOException {
            this.file = new RandomAccessFile(path.toFile(), "rw");
            this.channel = file.getChannel();
        }

        private long positionOfActualRecord = 0;

        private boolean finished = false;

        protected String nextRecord() {
            if (finished) {
                return null;
            }

            StringBuilder sb = new StringBuilder();
            try {
                positionOfActualRecord = channel.position();

                ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
                boolean deleted = false;
                boolean newRecord = true;
                int read;
                while ((read = channel.read(buffer)) > 0) {
                    buffer.flip();

                    for (int i = 0; i < read; i++) {
                        char ch = (char) buffer.get();

                        if (deleted) {
                            if (ch == RECORD_SEPARATOR) {
                                deleted = false;
                                newRecord = true;
                            }
                            continue;
                        }

                        if (newRecord && ch == RECORD_DELETED) {
                            deleted = true;
                            continue;
                        }

                        newRecord = false;

                        if (ch == RECORD_SEPARATOR) {
                            channel.position(channel.position() - (read - i - 1));

                            return sb.toString();

                        } else if (!deleted) {
                            sb.append(ch);
                        }
                    }
                    buffer = ByteBuffer.allocate(BUFFER_SIZE);
                }
                if (read <= 0) {
                    finished = true;
                }
            } catch (IOException e) {
                throw new DatabaseException("Cannot read a collection: " + e.getMessage(), e);
            }
            return null;
        }

        protected void deleteRecord() throws IOException {
            // write a "deleted" flag as the first byte
            channel.write(ByteBuffer.wrap(new byte[]{RECORD_DELETED}), positionOfActualRecord);
        }

        @Override
        public void close() {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.getStackTrace();
                    // ignore
                }
            }
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    // ignore
                }
            }
        }
    }

    class Select extends DbAccess {

        private final Where where;
        private final String elementKey;

        public Select(String elementKey, String where) throws IOException {
            super();
            this.where = Where.create(where);
            this.elementKey = elementKey;
        }

        public String next() {
            String json = nextRecord();

            if (json != null) {
                if (where.matches(json)) {
                    return selectElement(deserialize(json));

                } else {
                    return next();
                }
            }
            return null;
        }

        private String selectElement(String jsonObject) {
            if ("*".equals(elementKey) || elementKey == null || elementKey.isEmpty()) {
                return jsonObject;
            }
            Object o = new TSONObject(jsonObject).findByPath(elementKey);

            if (o == null) {
                return new TSONObject().toString();
            }
            if (o instanceof TSONObject) {
                return ((TSONObject) o).toString();
            }
            TSONObject tson = new TSONObject();
            tson.put(getLastKey(elementKey), o);
            return tson.toString();
        }

        private String getLastKey(String elementKey) {
            int index = elementKey.lastIndexOf(".");
            if (index != -1) {
                return elementKey.substring(index + 1);
            }
            return elementKey;
        }

    }

    class Delete extends DbAccess {

        private final Where where;

        public Delete(String where) throws IOException {
            super();
            this.where = Where.create(where);
        }

        public boolean delete() throws IOException {
            boolean deleted = false;
            String json;
            do {
                json = nextRecord();

                if (json != null) {
                    if (where.matches(json)) {
                        delete(json);
                        deleteRecord();

                        deleted = true;
                    }
                }
            } while (json != null);

            close();

            return deleted;
        }

        private void delete(String json) throws IOException {
            // TODO remove from indexes etc
        }
    }

    class Update extends DbAccess {

        private final Where where;
        private final String[] columns;
        private final String[] values;

        public Update(String[] columns, String[] values, String where) throws IOException {
            super();
            this.where = Where.create(where);
            this.columns = columns;
            this.values = values;
        }

        public int update() throws IOException {
            int updated = 0;
            String json;
            do {
                json = nextRecord();

                if (json != null) {
                    if (where.matches(json)) {
                        json = update(json, columns, values);
                        deleteRecord();
                        insert(json);

                        updated++;
                    }
                }
            } while (json != null);

            return updated;
        }

        private String update(String json, String[] columns, String[] values) {
            TSONObject tson = new TSONObject(json);

            for (int i = 0; i < columns.length; i++) {
                Object value = null;
                if (values.length > i) {
                    value = getJsonValue(values[i]);
                }
                tson = tson.updateByPath(columns[i], value);
            }

            // TODO update indexes etc

            return tson.toString();
        }

        private Object getJsonValue(String value) {
            try {
                return new JSONObject(value);
            } catch (Exception ignore) {
            }
            try {
                return new JSONArray(value);
            } catch (Exception ignore) {
            }
            try {
                return new JSONObject("{\"value\":" + value + "}").get("value");
            } catch (Exception ignore) {
            }
            return value;
        }
    }
}
