package cz.net21.ttulka.thistledb.client;

/**
 * Created by ttulka
 * <p>
 * Exception thrown from the server.
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
