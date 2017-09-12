package cz.net21.ttulka.thistledb.server.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;

import lombok.extern.apachecommons.CommonsLog;

/**
 * Indexing of the database.
 *
 * @author ttulka
 */
@CommonsLog
class Indexing {

    static final String VALUE_SEPARATOR = "+";
    static final String POSITION_SEPARATOR = ",";

    static final char RECORD_SEPARATOR = '\1';
    static final char RECORD_DELETED = '\2';

    private final Path path;

    public Indexing(Path path) {
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

    public boolean exists(String index) {
        return Files.exists(getPathToIndex(index));
    }

    public Set<Long> positions(String index, String value) {
        Path pathToIndexValue = getPathToIndexValue(index, value);

        if (!Files.exists(path.resolve(pathToIndexValue))) {
            return Collections.emptySet();
        }

        Set<Long> positions = new TreeSet<>();
        try (SeekableByteChannel channel = Files.newByteChannel(pathToIndexValue, StandardOpenOption.READ)) {
            String record;
            while ((record = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_DELETED)) != null) {
                int separator = record.indexOf(VALUE_SEPARATOR);

                String val = record.substring(0, separator);
                if (val.equals(value)) {
                    String positionList = record.substring(separator + 1);

                    for (String pos : positionList.split(POSITION_SEPARATOR)) {
                        positions.add(Long.valueOf(pos));
                    }
                }
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot read an index file: " + pathToIndexValue, e);
        }
        return positions;
    }

    public void insert(String index, Object value, long position) {
        if (!exists(index)) {
            return;
        }
        Path pathToIndexValue = getPathToIndexValue(index, value.toString());
        try {
            Files.createDirectories(pathToIndexValue.getParent());

            try (SeekableByteChannel channel = Files.newByteChannel(pathToIndexValue, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                Set<String> savedPositions = new TreeSet<>();
                savedPositions.add(String.valueOf(position));

                String record;
                while ((record = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_DELETED)) != null) {
                    int separator = record.indexOf(VALUE_SEPARATOR);

                    String val = record.substring(0, separator);
                    if (val.equals(value)) {
                        // save old positions
                        String positionList = record.substring(separator + 1);
                        savedPositions.addAll(Arrays.asList(positionList.split(POSITION_SEPARATOR)));

                        // delete an old record
                        deleteRecord(channel, record);
                        break;
                    }
                }
                
                // add the new value
                channel.position(channel.size());   // append
                String positions = String.join(POSITION_SEPARATOR, savedPositions).replace(" ", "");
                String insert = value + VALUE_SEPARATOR + positions + RECORD_SEPARATOR;
                channel.write(ByteBuffer.wrap(insert.getBytes()));
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create an index file: " + pathToIndexValue, e);
        }
    }

    public void delete(String index, Object value, long position) {
        Path pathToIndexValue = getPathToIndexValue(index, value.toString());
        if (!Files.exists(pathToIndexValue)) {
            return;
        }
        String positionString = String.valueOf(position);
        try (SeekableByteChannel channel = Files.newByteChannel(pathToIndexValue, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            Set<String> savedPositions = new TreeSet<>();

            String record;
            while ((record = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_DELETED)) != null) {
                int separator = record.indexOf(VALUE_SEPARATOR);

                String val = record.substring(0, separator);
                if (val.equals(value)) {
                    // save old positions
                    String positionList = record.substring(separator + 1);
                    List<String> positions = Arrays.asList(positionList.split(POSITION_SEPARATOR));
                    if (positions.contains(positionString)) {
                        savedPositions.addAll(positions);
                        savedPositions.remove(positionString);

                        // delete an old record
                        deleteRecord(channel, record);
                    }
                    break;
                }
            }

            if (!savedPositions.isEmpty()) {
                // add save values without the deleted one
                channel.position(channel.size());   // append
                String positions = String.join(POSITION_SEPARATOR, savedPositions).replace(" ", "");
                String insert = value + VALUE_SEPARATOR + positions + RECORD_SEPARATOR;
                channel.write(ByteBuffer.wrap(insert.getBytes()));
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create an index file: " + pathToIndexValue, e);
        }
    }

    private void deleteRecord(SeekableByteChannel channel, String record) throws IOException {
        long pos = channel.position();
        channel.position(pos - record.length() - 1);
        channel.write(ByteBuffer.wrap(new byte[]{RECORD_DELETED}));
        channel.position(pos);
    }

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

    public void cleanUp(String index) {
        if (!exists(index)) {
            return;
        }
        cleanUpDirectory(getPathToIndex(index));
    }

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
        log.info("Clean up indexes for " + file); // TODO debug
        try {
            Path temp = Paths.get(file + ".tmp");
            ChannelUtils.createNewFileOrTruncateExisting(temp);

            try (SeekableByteChannel in = Files.newByteChannel(file, StandardOpenOption.READ);
                 SeekableByteChannel out = Files.newByteChannel(temp, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                String record;
                while ((record = ChannelUtils.next(in, RECORD_SEPARATOR, RECORD_DELETED)) != null) {
                    String output = record + RECORD_SEPARATOR;
                    out.write(ByteBuffer.wrap(output.getBytes()));
                }
            }
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            throw new DatabaseException("Cannot clean up an index file: " + file, e);
        }
    }
}
