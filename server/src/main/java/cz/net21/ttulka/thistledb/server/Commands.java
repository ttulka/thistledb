package cz.net21.ttulka.thistledb.server;

/**
 * @author ttulka
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
    ADD,
    REMOVE,

    CREATE_INDEX,
    DROP_INDEX
}
