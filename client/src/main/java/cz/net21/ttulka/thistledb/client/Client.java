package cz.net21.ttulka.thistledb.client;

import java.net.Socket;

/**
 * Created by ttulka
 * <p>
 * Client driver.
 */
public class Client implements AutoCloseable {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9658;

    private final Socket socket;

    public Client() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    public Client(String host) {
        this(host, DEFAULT_PORT);
    }

    public Client(int port) {
        this(DEFAULT_HOST, port);
    }

    public Client(String host, int port) {
        try {
            this.socket = new Socket(host, port);

        } catch (Exception e) {
            throw new ClientException("Cannot open a socket on " + host + ":" + port + ".", e);
        }
    }

    // TODO this is just an inspiration
    protected void executeCommand() {
//        try (PrintWriter out = new PrintWriter(socket.getOutputStream())
//        ) {
//            out.println(command);
//
//        } catch (Exception e) {
//            throw new ClientException("Cannot send a command to socket.", e);
//        }
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (Throwable t) {
            throw new ClientException("Cannot close a socket.", t);
        }
    }
}
