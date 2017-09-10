package cz.net21.ttulka.thistledb.server.db;

/**
 * Exception thrown by the database access.
 *
 * @author ttulka
 */
public class DatabaseException extends RuntimeException {

    public DatabaseException() {
    }

    public DatabaseException(String message) {
        super(message);
    }

    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseException(Throwable cause) {
        super(cause);
    }
}
