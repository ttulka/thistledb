package cz.net21.ttulka.thistledb.client;

/**
 * Created by ttulka
 */
public class QueryException extends ClientException {

    public QueryException(String message) {
        super(message);
    }

    public QueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
