package cz.net21.ttulka.thistledb.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Utils for working with Channels.
 *
 * @author ttulka
 */
public final class ChannelUtils {

    private static final int BUFFER_SIZE = 1024;

    private ChannelUtils() {
    }

    /**
     * Convenient method.
     *
     * @see #next(SeekableByteChannel, char, char, Long)
     */
    public static String next(final SeekableByteChannel channel,
                              final char separatorFlag,
                              final char deletedFlag) throws IOException {
        return next(channel, separatorFlag, deletedFlag, null);
    }

    /**
     * Returns a next record from the channel based on separator- and deleted-flags.
     * The next record starts from the current position in the channel.
     *
     * @param channel       the channel
     * @param separatorFlag the separator flag separating records
     * @param deletedFlag   the delete flag for a deleted record to start with
     * @param maxPosition   the maximum of bytes to read if not null
     * @return the next record
     * @throws IOException
     */
    public static String next(final SeekableByteChannel channel,
                              final char separatorFlag,
                              final char deletedFlag,
                              final Long maxPosition) throws IOException {
        long position = channel.position();
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        StringBuilder sb = new StringBuilder();

        boolean deleted = false;
        boolean newRecord = true;
        int read;
        while ((read = channel.read(buffer)) > 0) {
            buffer.flip();

            for (int i = 0; i < read; i++) {
                if (maxPosition != null && ++position > maxPosition) {
                    return null;
                }

                char ch = (char) buffer.get();

                if (deleted) {
                    if (ch == separatorFlag) {
                        deleted = false;
                        newRecord = true;
                    }
                    continue;
                }

                if (newRecord && ch == deletedFlag) {
                    deleted = true;
                    continue;
                }

                newRecord = false;

                if (ch == separatorFlag) {
                    channel.position(channel.position() - (read - i - 1));

                    return sb.toString();

                } else if (!deleted) {
                    sb.append(ch);
                }
            }
            buffer = ByteBuffer.allocate(BUFFER_SIZE);
        }
        if (read <= 0) {
            return null;
        }
        return next(channel, separatorFlag, deletedFlag);
    }

    /**
     * Create a new file or truncate it if already exists.
     * @param path the file
     * @throws IOException
     */
    public static void createNewFileOrTruncateExisting(Path path) throws IOException {
        Files.newByteChannel(path,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING
        ).close();
    }
}
