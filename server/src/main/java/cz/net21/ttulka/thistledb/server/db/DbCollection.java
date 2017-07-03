package cz.net21.ttulka.thistledb.server.db;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import cz.net21.ttulka.thistledb.tson.TSONObject;
import lombok.NonNull;

/**
 * Created by ttulka
 */
// TODO
public class DbCollection {

    static final char SEPARATOR = '\1';

    private final Path path;

    public DbCollection(@NonNull Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    public Select select(@NonNull String columns, String where) {
        try {
            return new Select();

        } catch (FileNotFoundException e) {
            throw new DatabaseException("Cannot work with a collection: " + e.getMessage(), e);
        }
    }

    public void insert(@NonNull TSONObject data) {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.APPEND)) {
            writer.write(data.toString());
            writer.write(SEPARATOR);

        } catch (IOException e) {
            throw new DatabaseException("Cannot insert into a collection: " + e.getMessage(), e);
        }
    }

    public boolean delete(String where) {
        try {
            new PrintWriter(Files.newOutputStream(path)).close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    class Select implements AutoCloseable {

        static final int BUFFER_SIZE = 1024;

        private final RandomAccessFile file;
        private final FileChannel channel;

        private boolean finished = false;

        public Select() throws FileNotFoundException {
            file = new RandomAccessFile(path.toFile(), "r");
            channel = file.getChannel();
        }

        TSONObject next() {
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

                        if (ch == SEPARATOR) {
                            channel.position(channel.position() - (read - i - 1));
                            return new TSONObject(sb.toString());

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
}
