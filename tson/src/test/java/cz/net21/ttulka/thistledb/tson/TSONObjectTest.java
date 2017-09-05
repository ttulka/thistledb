package cz.net21.ttulka.thistledb.tson;

import org.json.JSONArray;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author ttulka
 */
public class TSONObjectTest {

    private static final String JSON = "{\"a\":1,\"b\":{\"aa\":11,\"bb\":22,\"cc\":{\"aaa\":{\"aaaa\":1111,\"bbbb\":2222,\"c\":3333},\"bbb\":222,\"ccc\":333}},\"c\":3}";

    @Test
    public void shouldKeepOrderOfElementsTest() {
        assertThat("Should keep order of elements.", new TSONObject(JSON).toString(), is(JSON));
    }

    @Test
    public void findByPathTest() {
        TSONObject tson = new TSONObject(JSON);
        assertThat("Should find a path.", tson.findByPath("b.cc.aaa.c"), is(3333));
    }

    @Test
    public void updateByPathTest() {
        TSONObject tson = new TSONObject(JSON);
        TSONObject tsonUpdated = tson.updateByPath("b.cc.aaa.c", 666);
        assertThat("Should update a path.", tsonUpdated.findByPath("b.cc.aaa.c"), is(666));
        assertThat("Shouldn't update a path of the old object.", tson.findByPath("b.cc.aaa.c"), is(3333));

        tsonUpdated = tson.updateByPath("a", new TSONObject("{\"x\":123}"));
        assertThat("Should update a path with an object.", tsonUpdated.findByPath("a.x"), is(123));

        tsonUpdated = tson.updateByPath("a", new JSONArray("[1,2,3]"));
        assertThat("Should update a path with an array.", tsonUpdated.findByPath("a").toString(), is("[1,2,3]"));

        tsonUpdated = tson.updateByPath("q.w.e.r.y", "u");
        assertThat("Should create a path.", tsonUpdated.findByPath("q.w.e.r.y").toString(), is("u"));
    }
}
