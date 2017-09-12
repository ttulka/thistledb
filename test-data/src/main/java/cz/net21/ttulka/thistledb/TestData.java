package cz.net21.ttulka.thistledb;

import cz.net21.ttulka.thistledb.tson.TSONObject;

/**
 * Data for testing purposes.
 *
 * @author ttulka
 */
public abstract class TestData {

    public static final String JSON_BASIC = "{\"a\":\"1\"}";

    public static final String JSON_PERSON = "{\"person\":{\"name\":\"John\",\"surname\":\"Smith\",\"age\":42}}";

    public static final TSONObject TSON_BASIC = new TSONObject(JSON_BASIC);

    public static final TSONObject TSON_PERSON = new TSONObject(JSON_PERSON);
}
