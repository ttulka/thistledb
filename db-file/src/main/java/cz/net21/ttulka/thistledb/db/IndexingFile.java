package cz.net21.ttulka.thistledb.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;

import lombok.extern.apachecommons.CommonsLog;

/**
 * Indexing of the database files.
 * <p>
 * Indexed values are saved in a directory structure based on the value hash (@see #getValueHash(String)):<br>
 * `<collectionName>_idx/<index>/<hash[0]><hash[1]>/<hash[2]><hash[3]>/<hash[4]>/index`.
 * <p>
 * Index files contain records in form: `<value>#VALUE_SEPARATOR<position1>[#POSITION_SEPARATOR<positionX>]#RECORD_SEPARATOR`.<br>
 * Example:<br>
 *     `John+213,545,89961|Peter+100,76954,.9984,1024455|.ames+12,55551|.homas+.892`<br>
 * <p>
 * For performance reasons are positions NOT updated and deleted directly.<br>
 * When a position from a position set is deleted, the position value starts with `#POSITION_DELETED`.<br>
 * When a whole record is deleted (has only deleted positions), the record starts with `#RECORD_DELETED`.<br>
 * <p>
 * Clean up procedure collects all the positions for all values and puts them into one record.
 *
 * @author ttulka
 */
@CommonsLog
class IndexingFile implements Indexing {

    static final char RECORD_SEPARATOR = '\1';
    static final char RECORD_DELETED = '\2';

    static final String VALUE_SEPARATOR = "\3";
    static final String POSITION_SEPARATOR = ",";
    static final String POSITION_DELETED = RECORD_DELETED + "";

    final Path path;

    public IndexingFile(Path path) {
        this.path = Paths.get(path + "_idx");
    }

    private String getIndexHash(String index) {
        return index.replaceAll("%s", "");
    }

    private String getValueHash(String value) {
        return new StringBuilder()
                .append("0000")
                .append(Math.abs(value.hashCode()))
                .reverse().toString();
    }

    private Path getPathToIndex(String index) {
        String hash = getIndexHash(index);
        return path.resolve(hash);
    }

    private Path getPathToIndexValue(String index, String value) {
        String hash = getValueHash(value);
        return getPathToIndex(index)
                .resolve(hash.substring(0, 2))
                .resolve(hash.substring(2, 4))
                .resolve(hash.substring(4, 5))
                .resolve("index");
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public boolean exists(String index) {
        return Files.exists(getPathToIndex(index));
    }

    @Override
    public Set<Long> positions(String index, String value) {
        Path pathToIndex = getPathToIndex(index);

        if (!Files.exists(pathToIndex)) {
            return null;
        }

        Path pathToIndexValue = getPathToIndexValue(index, value);

        if (!Files.exists(pathToIndexValue)) {
            return Collections.emptySet();
        }

        Set<Long> positions = new TreeSet<>();
        try (SeekableByteChannel channel = Files.newByteChannel(pathToIndexValue, StandardOpenOption.READ)) {
            int separator;
            String val;
            String positionList;

            String record;
            while ((record = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_DELETED)) != null) {
                separator = record.indexOf(VALUE_SEPARATOR);

                val = record.substring(0, separator);
                if (val.equals(value)) {
                    positionList = record.substring(separator + 1);

                    for (String tokenPosition : positionList.split(POSITION_SEPARATOR)) {
                        if (!tokenPosition.startsWith(POSITION_DELETED)) {
                            positions.add(Long.valueOf(tokenPosition));
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot read an index file: " + pathToIndexValue, e);
        }
        return positions;
    }

    @Override
    public void insert(String index, Object value, long position) {
        if (!exists(index)) {
            return;
        }
        Path pathToIndexValue = getPathToIndexValue(index, value.toString());
        try {
            Files.createDirectories(pathToIndexValue.getParent());

            try (SeekableByteChannel channel = Files.newByteChannel(pathToIndexValue, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                channel.position(channel.size());   // append
                String insert = value + VALUE_SEPARATOR + position + RECORD_SEPARATOR;
                channel.write(ByteBuffer.wrap(insert.getBytes()));
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create an index file: " + pathToIndexValue, e);
        }
    }

    @Override
    public void delete(String index, Object value, long position) {
        Path pathToIndexValue = getPathToIndexValue(index, value.toString());
        if (!Files.exists(pathToIndexValue)) {
            return;
        }
        String positionString = String.valueOf(position);
        try (SeekableByteChannel channel = Files.newByteChannel(pathToIndexValue, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            int separator;
            String val;
            String positionList;
            String[] positions;
            int offset;

            String record;
            while ((record = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_DELETED)) != null) {
                separator = record.indexOf(VALUE_SEPARATOR);

                val = record.substring(0, separator);
                if (val.equals(value)) {
                    // delete old positions
                    positionList = record.substring(separator + 1);
                    positions = positionList.split(POSITION_SEPARATOR);

                    // only one position in the record
                    if (positions.length == 1 && positionString.equals(positions[0])) {
                        deleteRecord(channel, record);  // delete whole record
                        return;
                    }

                    // delete the position in the record
                    offset = 0;
                    for (String tokenPosition : positions) {
                        if (positionString.equals(tokenPosition)) {

                            deletePosition(channel, record, separator, offset);
                            return; // active position can occur only once
                        }
                        offset += tokenPosition.length() + 1;
                    }
                }
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot delete an index position: " + pathToIndexValue, e);
        }
    }

    private void deleteRecord(SeekableByteChannel channel, String record) throws IOException {
        long pos = channel.position();
        channel.position(pos - record.length() - 1);
        channel.write(ByteBuffer.wrap(new byte[]{RECORD_DELETED}));
        channel.position(pos);
    }

    private void deletePosition(SeekableByteChannel channel, String record, int separatorPos, int offset) throws IOException {
        long pos = channel.position();
        channel.position(pos - record.length() + separatorPos + offset);
        channel.write(ByteBuffer.wrap(new byte[]{RECORD_DELETED}));
        channel.position(pos);
    }

    @Override
    public boolean create(String index) {
        if (exists(index)) {
            return false;
        }
        try {
            Files.createDirectories(getPathToIndex(index));

        } catch (IOException e) {
            throw new DatabaseException("Cannot create an index directory: " + getPathToIndex(index), e);
        }
        return true;
    }

    @Override
    public void drop(String index) {
        if (!exists(index)) {
            return;
        }
        try {
            FileUtils.deleteDirectory(getPathToIndex(index).toFile());

        } catch (IOException e) {
            throw new DatabaseException("Cannot delete an index directory: " + getPathToIndex(index), e);
        }
    }

    @Override
    public void dropAll() {
        if (!Files.exists(path)) {
            return;
        }
        try {
            FileUtils.deleteDirectory(path.toFile());

        } catch (IOException e) {
            throw new DatabaseException("Cannot delete an index directory: " + path, e);
        }
    }

    @Override
    public void dropOnlyData() {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> filesStream = Files.walk(path)) {
            filesStream
                    .filter(Files::isRegularFile)
                    .filter(file -> file.endsWith("index"))
                    .forEach(this::deleteFile);

        } catch (IOException e) {
            throw new DatabaseException("Cannot clean up an index directory: " + path, e);
        }
    }

    private void deleteFile(Path file) {
        try {
            Files.delete(file);

        } catch (IOException e) {
            throw new DatabaseException("Cannot delete an index file: " + file, e);
        }
    }

    @Override
    public void cleanUp(String index) {
        if (!exists(index)) {
            return;
        }
        cleanUpDirectory(getPathToIndex(index));
    }

    @Override
    public void cleanUp() {
        if (!Files.exists(path)) {
            return;
        }
        cleanUpDirectory(path);
    }

    void cleanUpDirectory(Path dir) {
        try (Stream<Path> filesStream = Files.walk(dir)) {
            filesStream
                    .filter(Files::isRegularFile)
                    .filter(file -> file.endsWith("index"))
                    .forEach(this::cleanUpIndex);

        } catch (IOException e) {
            throw new DatabaseException("Cannot clean up an index directory: " + dir, e);
        }
    }

    void cleanUpIndex(Path file) {
        log.debug("Cleaning up indexes for " + file);
        try {
            Path temp = Paths.get(file + ".tmp");
            ChannelUtils.createNewFileOrTruncateExisting(temp);

            Map<String, Set<String>> valuesMap = new HashMap<>();

            // collect values' positions
            try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
                int separator;
                String value;
                String positionList;
                String[] positions;
                Set<String> set;

                String record;
                while ((record = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_DELETED)) != null) {
                    separator = record.indexOf(VALUE_SEPARATOR);
                    value = record.substring(0, separator);
                    positionList = record.substring(separator + 1);
                    positions = positionList.split(POSITION_SEPARATOR);

                    valuesMap.putIfAbsent(value, new TreeSet<>());
                    set = valuesMap.get(value);

                    for (String position : positions) {
                        if (!position.startsWith(POSITION_DELETED)) {
                            set.add(position);
                        }
                    }
                }
            }

            // write the positions
            try (SeekableByteChannel channel = Files.newByteChannel(temp, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                String positions;

                for (Map.Entry<String, Set<String>> entry : valuesMap.entrySet()) {
                    positions = String.join(POSITION_SEPARATOR, entry.getValue()).replace(" ", "");
                    String insert = entry.getKey() + VALUE_SEPARATOR + positions + RECORD_SEPARATOR;
                    channel.write(ByteBuffer.wrap(insert.getBytes()));
                }
            }

            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            throw new DatabaseException("Cannot clean up an index file: " + file, e);
        }
    }
}
