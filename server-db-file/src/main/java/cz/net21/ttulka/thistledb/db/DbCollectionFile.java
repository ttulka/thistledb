package cz.net21.ttulka.thistledb.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.json.JSONArray;
import org.json.JSONObject;

import cz.net21.ttulka.thistledb.tson.TSONObject;
import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Collection implementation for the file-access.
 *
 * @author ttulka
 */
@CommonsLog
public class DbCollectionFile implements DbCollection {

    static final char RECORD_SEPARATOR = '\1';
    static final char RECORD_DELETED = '\2';

    protected final Path path;

    private final Indexing indexing;

    public DbCollectionFile(@NonNull Path path) {
        this.path = path;
        this.indexing = new Indexing(path);
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
        try (Insert insert = new Insert()) {
            insert.insert(jsonData);

        } catch (IOException e) {
            throw new DatabaseException("Cannot insert into a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected Insert newInsert() throws IOException {
        return new Insert();
    }

    @Override
    public int delete(String where) {
        lock.writeLock().lock();
        try (Delete delete = new Delete(where)) {
            return delete.delete();

        } catch (IOException e) {
            throw new DatabaseException("Cannot delete from a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int update(String[] columns, String[] values, String where) {
        lock.writeLock().lock();
        try (Update update = new Update(columns, values, where)) {
            return update.update();

        } catch (IOException e) {
            throw new DatabaseException("Cannot update a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean createIndex(String column) {
        if (!indexing.exists(column)) {
            lock.writeLock().lock();
            try (CreateIndex createIndex = new CreateIndex(column)) {
                return true;

            } catch (IOException e) {
                throw new DatabaseException("Cannot create an insert for a collection: " + e.getMessage(), e);
            } finally {
                lock.writeLock().unlock();
            }
        }
        return false;
    }

    @Override
    public boolean dropIndex(String column) {
        if (indexing.exists(column)) {
            lock.writeLock().lock();
            try {
                new DropIndex(column);
                return true;

            } catch (IOException e) {
                throw new DatabaseException("Cannot drop an insert for a collection: " + e.getMessage(), e);
            } finally {
                lock.writeLock().unlock();
            }
        }
        return false;
    }

    @Override
    public void cleanUp() {
        lock.writeLock().lock();
        try (CleanUp cleanUp = new CleanUp()) {
            indexing.cleanUp();

        } catch (IOException e) {
            throw new DatabaseException("Cannot clean up a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected void drop() {
        try {
            Files.delete(path);
            indexing.dropAll();

        } catch (IOException e) {
            throw new DatabaseException("Cannot drop a collection: " + e.getMessage(), e);
        }
    }

    String serialize(String tson) {
        return tson;
    }

    String deserialize(String tson) {
        return tson;
    }

    abstract class DbAccess implements AutoCloseable {

        protected final SeekableByteChannel channel;

        protected DbAccess() throws IOException {
            this.channel = Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        private long positionOfActualRecord = 0;
        private Long maxPosition = null;

        private boolean finished = false;

        private Map<Where, IndexingWhere> indexingWheres = new HashMap<>();

        protected long getPositionOfActualRecord() {
            return positionOfActualRecord;
        }

        protected void setUpMaxPosition() {
            try {
                maxPosition = channel.size();

            } catch (IOException e) {
                throw new DatabaseException("Cannot read a collection: " + e.getMessage(), e);
            }
        }

        protected void freeMaxPosition() {
            maxPosition = null;
        }

        protected String nextRecord(Where where) {
            // first, try indexes
            if (!Where.EMPTY.equals(where)) {
                indexingWheres.putIfAbsent(where, new IndexingWhere(where, indexing));
                IndexingWhere indexingWhere = indexingWheres.get(where);
                if (indexingWhere.isIndexed()) {
                    try {
                        String json;
                        do {
                            long position = indexingWhere.nextPosition();
                            if (position != -1) {
                                channel.position(position);
                                json = nextRecord();

                                if (where.matches(json)) {
                                    return json;
                                }
                            } else {
                                return null;
                            }
                        } while (json != null);

                    } catch (IOException e) {
                        throw new DatabaseException("Cannot read a collection: " + e.getMessage(), e);
                    }
                }
            }

            // full search
            String json;
            while ((json = nextRecord()) != null) {
                if (where.matches(json)) {
                    return json;
                }
            }
            return null;
        }

        protected String nextRecord() {
            if (finished) {
                return null;
            }
            try {
                String next = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_DELETED, maxPosition);
                if (next == null) {
                    finished = true;
                    positionOfActualRecord = channel.size();
                } else {
                    positionOfActualRecord = channel.position() - next.length() - 1;
                }
                return next;

            } catch (IOException e) {
                throw new DatabaseException("Cannot read a collection: " + e.getMessage(), e);
            }
        }

        protected void deleteRecord(String json) throws IOException {
            // write a "deleted" flag as the first byte
            long position = channel.position();
            channel.position(positionOfActualRecord);
            channel.write(ByteBuffer.wrap(new byte[]{RECORD_DELETED}));
            channel.position(position);

            deleteFromIndexes(json);
        }

        private void deleteFromIndexes(String json) throws IOException {
            TSONObject tson = new TSONObject(json);
            Iterator<String> columns = new ColumnsIterator(tson);
            while (columns.hasNext()) {
                String column = columns.next();
                Object value = tson.findByPath(column);

                indexing.delete(column, value, positionOfActualRecord);
            }
        }

        @Override
        public void close() {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignore) {
                    log.warn("Cannot close a channel.", ignore);
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

    class Insert extends DbAccess {

        protected Insert() throws IOException {
            super();
        }

        public void insert(Collection<String> jsonData) throws IOException {
            long currentPosition = channel.position();

            // append
            channel.position(channel.size());

            for (String json : jsonData) {
                writeData(json);
            }
            channel.position(currentPosition);
        }

        public void insert(String jsonData) throws IOException {
            long currentPosition = channel.position();

            // append
            channel.position(channel.size());

            writeData(jsonData);

            channel.position(currentPosition);
        }

        private void writeData(String json) throws IOException {
            long position = channel.position();

            String data = serialize(json) + RECORD_SEPARATOR;
            channel.write(ByteBuffer.wrap(data.getBytes()));

            indexNewRecord(json, position);
        }

        private void indexNewRecord(String json, long position) {
            TSONObject tson = new TSONObject(json);
            Iterator<String> columns = new ColumnsIterator(tson);

            while (columns.hasNext()) {
                String column = columns.next();
                Object value = tson.findByPath(column);

                indexing.insert(column, value, position);
            }
        }
    }

    class Update extends Insert {

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
            setUpMaxPosition();

            int updated = 0;
            String json;
            while ((json = nextRecord(where)) != null) {
                String updatedJson = updateData(json, columns, values);
                if (updatedJson != null && !updatedJson.equals(json)) {
                    deleteRecord(json);
                    insert(updatedJson);

                    updated++;
                }
            }
            freeMaxPosition();

            return updated;
        }

        private String updateData(String json, String[] columns, String[] values) {
            TSONObject tson = new TSONObject(json);
            boolean updated = false;

            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];

                if (tson.findByPath(column) == null) {
                    continue;
                }
                updated = true;

                Object value = null;
                if (values.length > i) {
                    value = getJsonValue(values[i]);
                }
                tson = tson.updateByPath(column, value);
            }

            return updated ? tson.toString() : null;
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

    class Delete extends DbAccess {

        private final Where where;

        public Delete(String where) throws IOException {
            super();
            this.where = Where.create(where);
        }

        public int delete() throws IOException {
            int deleted = 0;
            String json;
            while ((json = nextRecord(where)) != null) {
                deleteRecord(json);
                deleted++;
            }
            close();
            return deleted;
        }
    }

    class CreateIndex extends DbAccess {

        public CreateIndex(String column) throws IOException {
            super();
            if (indexing.create(column)) {
                String json;
                while ((json = nextRecord()) != null) {
                    TSONObject tson = new TSONObject(json);
                    Object value = tson.findByPath(column);
                    if (value != null && !(value instanceof TSONObject)) {
                        indexing.insert(column, value, getPositionOfActualRecord());
                    }
                }
                indexing.cleanUp(column);
            }
        }
    }

    class DropIndex {

        public DropIndex(String column) throws IOException {
            super();
            indexing.drop(column);
        }
    }

    class CleanUp extends Insert {

        public CleanUp() throws IOException {
            super();
            // create a temp empty collection
            Path tempCollectionPath = Paths.get(path + ".tmp");
            ChannelUtils.createNewFileOrTruncateExisting(tempCollectionPath);
            DbCollectionFile tmpCollection = new DbCollectionFile(tempCollectionPath);

            // drop real indexing data and copy the indexing structure to the temp collection
            if (Files.exists(indexing.path)) {
                indexing.dropOnlyFiles();
                Files.move(indexing.path, tmpCollection.indexing.path);
                indexing.dropAll();
            }

            // fetch all data from the collection and insert into the temp collection
            // new indexes will be written automatically with the insert
            try (Insert tmpInsert = tmpCollection.newInsert()) {
                String record;
                while ((record = nextRecord()) != null) {
                    tmpInsert.insert(record);
                }
            }
            // exchange the data and indexing
            close();
            Files.move(tmpCollection.path, path, StandardCopyOption.REPLACE_EXISTING);
            if (Files.exists(tmpCollection.indexing.path)) {
                Files.move(tmpCollection.indexing.path, indexing.path, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}
