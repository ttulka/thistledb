package cz.net21.ttulka.thistledb.server.db;

import cz.net21.ttulka.thistledb.server.ServerException;

/**
 * Exception thrown by the database access.
 *
 * @author ttulka
 */
public class DatabaseException extends ServerException {

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
