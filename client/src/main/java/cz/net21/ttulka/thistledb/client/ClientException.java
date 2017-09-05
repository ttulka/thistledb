package cz.net21.ttulka.thistledb.client;

/**
 * Exception thrown from the server.
 * <p>
 * @author ttulka
 */
public class ClientException extends RuntimeException {

    public ClientException() {
    }

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClientException(Throwable cause) {
        super(cause);
    }
}
