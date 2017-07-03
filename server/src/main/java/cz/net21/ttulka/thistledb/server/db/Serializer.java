package cz.net21.ttulka.thistledb.server.db;

import cz.net21.ttulka.thistledb.tson.TSONObject;

/**
 * Created by ttulka
 *
 * Serializer for JSON into a file.
 */
class Serializer {

    public static String serialize(TSONObject tson) {
        return tson.toString(); // TODO
    }

    public static TSONObject deserialize(String tson) {
        return new TSONObject(tson); // TODO
    }
}
