package cz.net21.ttulka.thistledb.server.db;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import cz.net21.ttulka.thistledb.tson.TSONObject;
import lombok.NonNull;

/**
 * Created by ttulka
 */
// TODO
public class DbCollection {

    static final char RECORD_SEPARATOR = '\1';

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

        } catch (FileNotFoundException e) {
            throw new DatabaseException("Cannot work with a collection: " + e.getMessage(), e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void insert(@NonNull TSONObject data) {
        lock.writeLock().lock();
        try {
            insert(serialize(data));
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void insert(String json) {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.APPEND)) {
            writer.write(json);
            writer.write(RECORD_SEPARATOR);

        } catch (IOException e) {
            throw new DatabaseException("Cannot insert into a collection: " + e.getMessage(), e);
        }
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

    String serialize(TSONObject tson) {
        return tson.toString();
    }

    TSONObject deserialize(String tson) {
        return new TSONObject(tson);
    }

    abstract class DbAccess implements AutoCloseable {

        static final int BUFFER_SIZE = 1024;

        protected final RandomAccessFile file;
        protected final FileChannel channel;

        protected DbAccess() throws FileNotFoundException {
            this.file = new RandomAccessFile(path.toFile(), "r");
            this.channel = file.getChannel();
        }

        private boolean finished = false;

        protected String nextRecord() {
            if (finished) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            try {
                ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
                int read;
                while ((read = channel.read(buffer)) > 0) {
                    buffer.flip();

                    for (int i = 0; i < read; i++) {
                        char ch = (char) buffer.get();

                        if (ch == RECORD_SEPARATOR) {
                            channel.position(channel.position() - (read - i - 1));

                            return sb.toString();

                        } else {
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

        @Override
        public void close() {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    class Select extends DbAccess {

        private final Where where;
        private final String elementKey;

        public Select(String elementKey, String where) throws FileNotFoundException {
            super();
            this.where = Where.create(where);
            this.elementKey = elementKey;
        }

        public TSONObject next() {
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

        private TSONObject selectElement(TSONObject jsonObject) {
            if ("*".equals(elementKey) || elementKey == null || elementKey.isEmpty()) {
                return jsonObject;
            }
            Object o = jsonObject.findPath(elementKey);

            if (o == null) {
                return new TSONObject();
            }
            if (o instanceof TSONObject) {
                return (TSONObject) o;
            }
            TSONObject tson = new TSONObject();
            tson.put(getLastKey(elementKey), o);
            return tson;
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

        public Delete(String where) throws FileNotFoundException {
            super();
            this.where = Where.create(where);
        }

        public boolean delete() throws IOException {
            boolean deleted = false;
            DbCollection tmpCollection = new DbCollection(Files.createTempFile(path.getParent(), null, "_delete"));
            String json;
            do {
                json = nextRecord();

                if (json != null) {
                    if (where.matches(json)) {
                        delete(json);
                        deleted = true;
                    } else {
                        tmpCollection.insert(json);
                    }
                }
            } while (json != null);

            moveCollection(tmpCollection);

            return deleted;
        }

        private void moveCollection(DbCollection tmpCollection) throws IOException {
            close();
            Files.move(tmpCollection.getPath(), path, StandardCopyOption.REPLACE_EXISTING);
        }

        private void delete(String json) {
            // TODO remove from indexes etc
        }
    }
}
