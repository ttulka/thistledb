package cz.net21.ttulka.thistledb.server.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;

/**
 * Indexing of the database.
 *
 * @author ttulka
 */
class Indexing {

    private static final String VALUE_SEPARATOR = "+";
    private static final String POSITION_SEPARATOR = ",";

    static final char RECORD_SEPARATOR = '\1';
    static final char RECORD_DELETED = '\2';

    private final Path path;

    public Indexing(Path path) {
        this.path = Paths.get(path + "_idx");

        createIndexingDirectory(this.path);
    }

    private void createIndexingDirectory(Path path) {
        if (!Files.exists(path)) {
            try {
                Files.createDirectory(path);

            } catch (IOException e) {
                throw new DatabaseException("Cannot create indexing directory '" + path + "'.");
            }
        }
    }

    private String getIndexHash(String index) {
        return index.replaceAll("%s", "").replace('.', '_');
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

        Set<Long> positions = new HashSet<>();
        try (SeekableByteChannel channel = Files.newByteChannel(pathToIndexValue, StandardOpenOption.READ)) {
            String line;
            while ((line = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_SEPARATOR)) != null) {
                int separator = line.indexOf(VALUE_SEPARATOR);

                String val = line.substring(0, separator);
                if (val.equals(value)) {
                    String positionList = line.substring(separator + 1);

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
                Set<String> savedPositions = new HashSet<>();
                savedPositions.add(String.valueOf(position));

                String line;
                while ((line = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_SEPARATOR)) != null) {
                    int separator = line.indexOf(VALUE_SEPARATOR);

                    String val = line.substring(0, separator);
                    if (val.equals(value)) {
                        // save old positions
                        String positionList = line.substring(separator + 1);
                        savedPositions.addAll(Arrays.asList(positionList.split(POSITION_SEPARATOR)));

                        // delete an old record
                        long pos = channel.position();
                        channel.position(pos - line.length());
                        channel.write(ByteBuffer.wrap(new byte[]{RECORD_DELETED}));
                        channel.position(pos);

                        break;
                    }
                }

                // add the new value
                channel.position(channel.size());   // append
                String positions = String.join(POSITION_SEPARATOR, savedPositions).replace(" ", "");
                String record = value + VALUE_SEPARATOR + positions + RECORD_SEPARATOR;
                channel.write(ByteBuffer.wrap(record.getBytes()));
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create an index file: " + pathToIndexValue, e);
        }
    }

    public void delete(String index, Object value, long position) {
        Path pathToIndexValue = getPathToIndexValue(index, value.toString());

        if (!Files.exists(path.resolve(pathToIndexValue))) {
            return;
        }
        try (SeekableByteChannel channel = Files.newByteChannel(pathToIndexValue, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            Set<String> savedPositions = new HashSet<>();

            String line;
            while ((line = ChannelUtils.next(channel, RECORD_SEPARATOR, RECORD_SEPARATOR)) != null) {
                int separator = line.indexOf(VALUE_SEPARATOR);

                String val = line.substring(0, separator);
                if (val.equals(value)) {
                    // save old positions
                    String positionList = line.substring(separator + 1);
                    List<String> positions = Arrays.asList(positionList.split(POSITION_SEPARATOR));
                    String positionString = String.valueOf(position);
                    if (positions.contains(positionString)) {
                        savedPositions.addAll(positions);
                        savedPositions.remove(positionString);

                        // delete an old record
                        long pos = channel.position();
                        channel.position(pos - line.length());
                        channel.write(ByteBuffer.wrap(new byte[]{RECORD_DELETED}));
                        channel.position(pos);
                    }
                    break;
                }
            }

            if (!savedPositions.isEmpty()) {
                // add save values without the deleted one
                channel.position(channel.size());   // append
                String positions = String.join(POSITION_SEPARATOR, savedPositions).replace(" ", "");
                String record = value + VALUE_SEPARATOR + positions + RECORD_SEPARATOR;
                channel.write(ByteBuffer.wrap(record.getBytes()));
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot create an index file: " + pathToIndexValue, e);
        }
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
        // TODO
    }

    public void cleanUp() {
        if (!Files.exists(path)) {
            return;
        }
        // TODO
    }
}
