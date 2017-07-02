package cz.net21.ttulka.thistledb.tson;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class TSONTest {

    private static final String JSON = "{\"a\":1,\"b\":{\"aa\":11,\"bb\":22,\"cc\":{\"aaa\":{\"aaaa\":1111,\"bbbb\":2222,\"c\":3333},\"bbb\":222,\"ccc\":333}},\"c\":3}";

    @Test
    public void shouldKeepOrderOfElementsTest() {
        assertThat("Should keep order of elements.", new TSONObject(JSON).toString(), is(JSON));
    }
}
