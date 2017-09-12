package cz.net21.ttulka.thistledb.tson;

import org.json.JSONArray;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author ttulka
 */
public class TSONObjectTest {

    private static final String JSON = "{" +
                                       "    \"a\":1," +
                                       "    \"b\":{" +
                                       "        \"aa\":11," +
                                       "        \"bb\":22," +
                                       "        \"cc\":{" +
                                       "            \"aaa\":{" +
                                       "                \"aaaa\":1111," +
                                       "                \"bbbb\":2222," +
                                       "                \"cccc\":3333" +
                                       "            }," +
                                       "            \"bbb\":222," +
                                       "            \"ccc\":333" +
                                       "        }" +
                                       "    }," +
                                       "    \"c\":3" +
                                       "}";

    @Test
    public void shouldKeepOrderOfElementsTest() {
        assertThat("Should keep order of elements.", new TSONObject(JSON).toString(), is(JSON.replaceAll("\\s", "")));
    }

    @Test
    public void findByPathTest() {
        TSONObject tson = new TSONObject(JSON);

        assertThat("Should find a path.", tson.findByPath("b.cc.aaa.cccc"), is(3333));
    }

    @Test
    public void updateByPathTest() {
        TSONObject tson = new TSONObject(JSON);

        TSONObject tsonUpdated = tson.updateByPath("b.cc.aaa.cccc", 666);
        assertThat("Should update a path.", tsonUpdated.findByPath("b.cc.aaa.cccc"), is(666));
        assertThat("Shouldn't update a path of the old object.", tson.findByPath("b.cc.aaa.cccc"), is(3333));

        tsonUpdated = tson.updateByPath("b.cc.aaa.bbbb", null);
        assertThat("Should update a path.", tsonUpdated.findByPath("b.cc.aaa.bbbb"), is(nullValue()));

        tsonUpdated = tson.updateByPath("b.cc.ccc", 666);
        assertThat("Should update a path.", tsonUpdated.findByPath("b.cc.ccc"), is(666));

        tsonUpdated = tson.updateByPath("a", new TSONObject("{\"x\":123}"));
        assertThat("Should update a path with an object.", tsonUpdated.findByPath("a.x"), is(123));

        tsonUpdated = tson.updateByPath("a", new JSONArray("[1,2,3]"));
        assertThat("Should update a path with an array.", tsonUpdated.findByPath("a").toString(), is("[1,2,3]"));

        tsonUpdated = tson.updateByPath("b.aa.xx", "XX");
        assertThat("Should create a path.", tsonUpdated.findByPath("b.aa.xx"), is(nullValue()));

        tsonUpdated = tson.updateByPath("q.w.e.r.y", "u");
        assertThat("Should create a path.", tsonUpdated.findByPath("q.w.e.r.y"), is(nullValue()));
    }

    @Test
    public void addByPathTest() {
        addByPath(new TSONObject("{}"));
        addByPath(new TSONObject(JSON));
    }

    private void addByPath(TSONObject tson) {
        TSONObject tsonUpdated = tson.addByPath("b.cc.aaa.x", 666);
        assertThat("Should update a path.", tsonUpdated.findByPath("b.cc.aaa.x"), is(666));
        assertThat("Shouldn't update a path of the old object.", tson.findByPath("b.cc.aaa.x"), is(nullValue()));

        tsonUpdated = tson.addByPath("b.cc.aaa.y.z", null);
        assertThat("Should update a path.", tsonUpdated.findByPath("b.cc.aaa.y"), not(nullValue()));
        assertThat("Should update a path.", tsonUpdated.findByPath("b.cc.aaa.y.z"), is(TSONObject.NULL));

        tsonUpdated = tson.addByPath("a", new TSONObject("{\"x\":123}"));
        assertThat("Should update a path with an object.", tsonUpdated.findByPath("a.x"), is(123));

        tsonUpdated = tson.addByPath("a", new JSONArray("[1,2,3]"));
        assertThat("Should update a path with an array.", tsonUpdated.findByPath("a").toString(), is("[1,2,3]"));

        tsonUpdated = tson.addByPath("q.w.e.r.y", "u");
        assertThat("Should create a path.", tsonUpdated.findByPath("q.w.e.r.y").toString(), is("u"));
    }

    @Test
    public void removeByPathTest() {
        TSONObject tson = new TSONObject(JSON);

        TSONObject tsonUpdated = tson.removeByPath("b.cc.aaa.cccc");
        assertThat("Should update a path.", tsonUpdated.findByPath("b.cc.aaa.cccc"), is(nullValue()));
        assertThat("Shouldn't remove anything else.", tsonUpdated.findByPath("b.cc.aaa.aaaa"), is(1111));
        assertThat("Shouldn't update a path of the old object.", tson.findByPath("b.cc.aaa.cccc"), is(3333));

        tsonUpdated = tson.removeByPath("a");
        assertThat("Should remove an element from the path.", tsonUpdated.findByPath("a"), is(nullValue()));

        tsonUpdated = tson.removeByPath("b");
        assertThat("Should remove an element from the path.", tsonUpdated.findByPath("b"), is(nullValue()));
        assertThat("Shouldn't remove anything else.", tsonUpdated.findByPath("c"), is(3));

        tsonUpdated = tson.removeByPath("q.w.e.r.y");
        assertThat("Should create a path.", tsonUpdated.findByPath("q.w.e.r.y"), is(nullValue()));
    }
}
