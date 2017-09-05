package cz.net21.ttulka.thistledb.server;

/**
 * @author ttulka
 * <p>
 * Exception thrown from the server.
 */
public class ServerException extends RuntimeException {

    public ServerException() {
    }

    public ServerException(String message) {
        super(message);
    }

    public ServerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServerException(Throwable cause) {
        super(cause);
    }
}
