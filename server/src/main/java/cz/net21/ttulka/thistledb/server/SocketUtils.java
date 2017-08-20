package cz.net21.ttulka.thistledb.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Utilities for working with sockets.
 * <p>
 * Created by ttulka
 */
class SocketUtils {

    /**
     * Prints a string to a socket channel as a new line.
     *
     * @param output  the string to be written
     * @param channel the socket channel to be written into.
     * @return true if all data was written, otherwise false
     */
    public static boolean printlnIntoChannel(String output, SocketChannel channel) {
        if (output != null) {
            ByteBuffer buffer = ByteBuffer.wrap((output + "\n").getBytes());
            while (buffer.hasRemaining()) {
                try {
                    channel.write(buffer);

                } catch (IOException e) {
                    return false;
                }
            }
        }
        return true;
    }
}
