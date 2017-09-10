package cz.net21.ttulka.thistledb.server.db;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.json.JSONArray;
import org.json.JSONObject;

import cz.net21.ttulka.thistledb.tson.TSONObject;
import lombok.NonNull;

/**
 * Collection implementation for the file-access.
 *
 * @author ttulka
 */
public class DbCollectionFile implements DbCollection {

    static final char RECORD_SEPARATOR = '\1';
    static final char RECORD_DELETED = '\2';

    protected final Path path;

    public DbCollectionFile(@NonNull Path path) {
        this.path = path;
    }

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public Iterator<String> select(@NonNull String element, String where) {
        lock.readLock().lock();
        try {
            return new Select(element, where);

        } catch (IOException e) {
            throw new DatabaseException("Cannot work with a collection: " + e.getMessage(), e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
    public void cleanUp() {
        lock.writeLock().lock();
        try {
            new CleanUp().cleanUp();

        } catch (IOException e) {
            throw new DatabaseException("Cannot clean up a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected void drop() {
        try {

            Files.delete(path);

        } catch (IOException e) {
            throw new DatabaseException("Cannot drop a collection: " + e.getMessage(), e);
        }
        // TODO drop indexes etc.
    }

    protected void move(Path newPath) {
        try {
            Files.move(path, newPath, StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            throw new DatabaseException("Cannot move a collection: " + e.getMessage(), e);
        }
        // TODO move indexes etc.
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

        protected String nextRecord(Where where) {
            String json;
            do {
                json = nextRecord();

                if (where.matches(json)) {
                    return json;
                }
            } while (json != null);

            return null;
        }

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

    class Select extends DbAccess implements Iterator<String> {

        private final Where where;
        private final String elementKey;

        private String next;

        public Select(String elementKey, String where) throws IOException {
            super();
            this.where = Where.create(where);
            this.elementKey = elementKey;

            next = getNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public String next() {
            if (next != null) {
                String toReturn = next;
                next = getNext();
                return toReturn;
            }
            return null;
        }

        private String getNext() {
            String json = nextRecord(where);

            if (json != null) {
                return selectElement(deserialize(json));
            }
            close();
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
                json = nextRecord(where);

                if (json != null) {
                    delete(json);
                    deleteRecord();

                    deleted = true;
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
                json = nextRecord(where);

                if (json != null) {
                    json = update(json, columns, values);
                    deleteRecord();
                    insert(json);

                    updated++;
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

    class CleanUp extends DbAccess {

        private DbCollectionFile tempCollection;

        public CleanUp() throws IOException {
            super();
            Path tempCollectionPath = Paths.get(path + ".tmp");
            createNewFileOrTruncateExisting(tempCollectionPath);

            this.tempCollection = new DbCollectionFile(tempCollectionPath);
        }

        private void createNewFileOrTruncateExisting(Path path) throws IOException {
            Files.newByteChannel(path,
                                 StandardOpenOption.WRITE,
                                 StandardOpenOption.CREATE,
                                 StandardOpenOption.TRUNCATE_EXISTING
            ).close();
        }

        public void cleanUp() {
            String record;
            while ((record = nextRecord()) != null) {
                tempCollection.insert(record);
            }
            close();
            tempCollection.move(path);
        }
    }
}
