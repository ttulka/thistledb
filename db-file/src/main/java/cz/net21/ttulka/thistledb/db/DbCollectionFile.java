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
import java.util.function.BiFunction;

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

    final Indexing indexing;

    // For performance reasons we hold the last opened writer in the memory. This is handy when a lot of same operations are proceed together.
    private Insert insertHolder;

    public DbCollectionFile(@NonNull Path path) {
        this.path = path;
        this.indexing = new IndexingFile(path);
    }

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public Iterator<String> select(@NonNull String element, String where) {
        lock.readLock().lock();
        try {
            return new Select(element, where);

        } catch (Exception e) {
            throw new DatabaseException("Cannot work with a collection: " + e.getMessage(), e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void insert(@NonNull Collection<String> jsonData) {
        lock.writeLock().lock();
        try {
            Insert insert = insertHolder != null ? insertHolder : (insertHolder = new Insert());
            insert.insert(jsonData);

        } catch (Exception e) {
            throw new DatabaseException("Cannot insertRecord into a collection: " + e.getMessage(), e);
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

        } catch (Exception e) {
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

        } catch (Exception e) {
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

            } catch (Exception e) {
                throw new DatabaseException("Cannot create an insertRecord for a collection: " + e.getMessage(), e);
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

            } catch (Exception e) {
                throw new DatabaseException("Cannot drop an insertRecord for a collection: " + e.getMessage(), e);
            } finally {
                lock.writeLock().unlock();
            }
        }
        return false;
    }

    @Override
    public int add(String element, String where) {
        lock.writeLock().lock();
        try (Alter alter = new Alter(where)) {
            return alter.add(element);

        } catch (Exception e) {
            throw new DatabaseException("Cannot alter (add) a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int remove(String element, String where) {
        lock.writeLock().lock();
        try (Alter alter = new Alter(where)) {
            return alter.remove(element);

        } catch (Exception e) {
            throw new DatabaseException("Cannot alter (add) a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void cleanUp() {
        lock.writeLock().lock();
        closeInsertHolder();
        try (CleanUp cleanUp = new CleanUp()) {
            indexing.cleanUp();

        } catch (Exception e) {
            throw new DatabaseException("Cannot clean up a collection: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected void drop() {
        closeInsertHolder();
        try {
            Files.delete(path);
            indexing.dropAll();

        } catch (Exception e) {
            throw new DatabaseException("Cannot drop a collection: " + e.getMessage(), e);
        }
    }

    String serialize(String tson) {
        return tson;
    }

    String deserialize(String tson) {
        return tson;
    }

    private void closeInsertHolder() {
        if (insertHolder != null) {
            insertHolder.close();
            insertHolder = null;
        }
    }

    /**
     * Abstract class for all database operations. Offers methods for reading, writing and deleting.
     */
    abstract class DbAccess implements AutoCloseable {

        protected final SeekableByteChannel channel;

        protected DbAccess() throws IOException {
            channel = Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
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

        protected String readNextRecord(Where where) {
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
                                json = readNextRecord();

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
            while ((json = readNextRecord()) != null) {
                if (where.matches(json)) {
                    return json;
                }
            }
            return null;
        }

        protected String readNextRecord() {
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

        protected void insertRecord(String jsonData) throws IOException {
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

            insertIntoIndexes(json, position);
        }

        private void insertIntoIndexes(String json, long position) {
            TSONObject tson = new TSONObject(json);
            Iterator<String> columns = new ColumnsIterator(tson);

            while (columns.hasNext()) {
                String column = columns.next();
                Object value = tson.findByPath(column);

                indexing.insert(column, value, position);
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

    final class Select extends DbAccess implements Iterator<String> {

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
            String json = readNextRecord(where);

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
            for (String json : jsonData) {
                insertRecord(json);
            }
        }
    }

    final class Update extends DbAccess {

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
            while ((json = readNextRecord(where)) != null) {
                String updatedJson = updateData(json, columns, values);
                if (updatedJson != null && !updatedJson.equals(json)) {
                    deleteRecord(json);
                    insertRecord(updatedJson);

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

    final class Delete extends DbAccess {

        private final Where where;

        public Delete(String where) throws IOException {
            super();
            this.where = Where.create(where);
        }

        public int delete() throws IOException {
            int deleted = 0;
            String json;
            while ((json = readNextRecord(where)) != null) {
                deleteRecord(json);
                deleted++;
            }
            close();
            return deleted;
        }
    }

    final class Alter extends DbAccess {

        private final Where where;

        public Alter(String where) throws IOException {
            super();
            this.where = Where.create(where);
        }

        public int add(String element) throws IOException {
            return alter(element, this::addData);
        }

        public int remove(String element) throws IOException {
            return alter(element, this::removeData);
        }

        private int alter(String element, BiFunction<String, String, String> alterFnc) throws IOException {
            setUpMaxPosition();

            int updated = 0;
            String json;
            while ((json = readNextRecord(where)) != null) {
                String updatedJson = alterFnc.apply(json, element);
                if (updatedJson != null) {
                    deleteRecord(json);
                    insertRecord(updatedJson);

                    updated++;
                }
            }
            freeMaxPosition();

            return updated;
        }

        private String addData(String json, String element) {
            TSONObject tson = new TSONObject(json);

            if (tson.findByPath(element) != null) {
                return null;
            }
            return tson.addByPath(element, null).toString();
        }

        private String removeData(String json, String element) {
            TSONObject tson = new TSONObject(json);

            if (tson.findByPath(element) == null) {
                return null;
            }
            return tson.removeByPath(element).toString();
        }
    }

    final class CreateIndex extends DbAccess {

        public CreateIndex(String column) throws IOException {
            super();
            if (indexing.create(column)) {
                String json;
                while ((json = readNextRecord()) != null) {
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

    final class DropIndex {

        public DropIndex(String column) {
            super();
            indexing.drop(column);
        }
    }

    final class CleanUp extends DbAccess {

        public CleanUp() throws IOException {
            super();
            // create a temp empty collection
            Path tempCollectionPath = Paths.get(path + ".tmp");
            ChannelUtils.createNewFileOrTruncateExisting(tempCollectionPath);
            DbCollectionFile tmpCollection = new DbCollectionFile(tempCollectionPath);

            // drop real indexing data and copy the indexing structure to the temp collection
            if (Files.exists(indexing.getPath())) {
                indexing.dropOnlyData();
                Files.move(indexing.getPath(), tmpCollection.indexing.getPath());
                indexing.dropAll();
            }

            // fetch all data from the collection and insertRecord into the temp collection
            // new indexes will be written automatically with the insertRecord
            try (Insert tmpInsert = tmpCollection.newInsert()) {
                String record;
                while ((record = readNextRecord()) != null) {
                    tmpInsert.insertRecord(record);
                }
            }
            // exchange the data and indexing
            close();
            Files.move(tmpCollection.path, path, StandardCopyOption.REPLACE_EXISTING);
            if (Files.exists(tmpCollection.indexing.getPath())) {
                Files.move(tmpCollection.indexing.getPath(), indexing.getPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}
