package cz.net21.ttulka.thistledb.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ttulka
 */
@CommonsLog
class ClientConnectionThread implements Runnable {

    private final SocketChannel clientChannel;
    private final Selector selector;
    private final QueryProcessor queryProcessor;

    private final StringBuilder input = new StringBuilder();
    private final List<String> output = new LinkedList<>();

    private boolean live = true;

    private final int number; // TODO remove, only for testing purposes

    ClientConnectionThread(int number, SocketChannel clientChannel, DataSource dataSource) throws IOException {
        this.number = number;
        this.clientChannel = clientChannel;
        this.selector = Selector.open();
        this.queryProcessor = new QueryProcessor(dataSource);

        clientChannel.register(selector, SelectionKey.OP_READ);
    }

    @Override
    public void run() {
        log.info("Running a new client connection thread " + number);   // TODO debug
        try {
            while (live) {
                selector.select();

                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove(); // prevent the same key from coming up again the next time around

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isReadable()) {
                        read(key);
                    } else if (key.isWritable()) {
                        write(key);
                    }
                }
            }
        } catch (Exception ignore) {
            log.warn("Exception by processing a client request.", ignore);

        } finally {
            close();
        }
    }

    public void stop() {
        live = false;
    }

    public boolean isLive() {
        return live;
    }

    private void read(SelectionKey key) throws IOException {
        log.info("Read from a client " + number);   // TODO debug
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int numRead;
        try {
            numRead = clientChannel.read(buffer);
        } catch (IOException e) {
            // remote forcibly closed the connection
            key.cancel();
            live = false;
            return;
        }

        if (numRead == -1) { // remote entity shut the socket down cleanly
            key.cancel();
            live = false;
            return;
        }

        byte[] data = new byte[numRead];
        System.arraycopy(buffer.array(), 0, data, 0, numRead);

        input.append(new String(data));

        processInput();
    }

    private String readLineFromBuffer() {
        if (input.length() > 0) {
            int newLinePosition = input.indexOf("\n");
            String line = input.substring(0, newLinePosition + 1);
            input.delete(0, newLinePosition + 1);

            return line.trim();
        }
        return null;
    }

    private void processInput() {
        String data;
        while ((data = readLineFromBuffer()) != null) {
            log.info("Processing a client input \"" + data + "\" connection thread " + number);   // TODO debug
            queryProcessor.process(data, this::processOutput);
        }
    }

    private void write(SelectionKey key) throws IOException {
        synchronized (output) {
            Iterator<String> iterator = output.iterator();
            while (iterator.hasNext()) {
                String data = iterator.next();
                log.info("Write \"" + data + "\" to a client " + number);   // TODO debug
                if (!SocketUtils.printlnIntoChannel(data, clientChannel)) {
                    break;
                }
                iterator.remove();
            }

            if (output.isEmpty()) {
                key.interestOps(SelectionKey.OP_READ); // all data are written, switch back to waiting for data
            }
        }
    }

    private void processOutput(String data) {
        if (!live) {
            return;
        }
        log.info("Processing a client output \"" + data + "\" connection thread " + number);   // TODO debug
        synchronized (output) {
            output.add(data);
        }

        SelectionKey key = clientChannel.keyFor(selector);
        key.interestOps(SelectionKey.OP_WRITE);

        // wake up our selecting thread so it can make the required changes
        selector.wakeup();  // TODO unnecessary???
    }

    private void close() {
        log.info("Closing a client socket " + number); // TODO debug
        live = false;
        try {
            clientChannel.close();
        } catch (IOException ignore) {
        }
        try {
            selector.close();
        } catch (IOException ignore) {
        }
    }
}
