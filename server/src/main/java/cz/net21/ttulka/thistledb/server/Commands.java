package cz.net21.ttulka.thistledb.server;

/**
 * Created by ttulka
 * <p>
 * Command to send to a server.
 */
enum Commands {

    SELECT,
    INSERT,
    UPDATE,
    DELETE,

    CREATE,
    DROP,

    CREATE_INDEX,
    DROP_INDEX
}
