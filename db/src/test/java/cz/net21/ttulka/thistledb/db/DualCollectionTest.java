package cz.net21.ttulka.thistledb.db;

import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.junit.Test;

import cz.net21.ttulka.thistledb.tson.TSONObject;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class DualCollectionTest {

    private DualCollection dual = DualCollection.getInstance();

    @Test
    public void selectAllTest() {
        Iterator<String> select = dual.select("*", null);

        assertThat(select, notNullValue());
        assertThat(select.hasNext(), is(false));
    }

    @Test
    public void selectIntegerTest() {
        Iterator<String> select = dual.select("123", null);

        assertThat(select, notNullValue());
        assertThat(select.hasNext(), is(true));

        String result = select.next();
        assertThat(result, notNullValue());
        assertThat(new TSONObject(result).get("value"), notNullValue());
        assertThat(new TSONObject(result).get("value"), is(123));

        assertThat(select.hasNext(), is(false));
    }

    @Test
    public void selectDoubleTest() {
        Iterator<String> select = dual.select("123.456", null);

        assertThat(select, notNullValue());
        assertThat(select.hasNext(), is(true));

        String result = select.next();
        assertThat(result, notNullValue());
        assertThat(new TSONObject(result).get("value"), notNullValue());
        assertThat(new TSONObject(result).get("value"), is(123.456));

        assertThat(select.hasNext(), is(false));
    }

    @Test
    public void selectBoolenTest() {
        Iterator<String> select = dual.select("true", null);

        assertThat(select, notNullValue());
        assertThat(select.hasNext(), is(true));

        String result = select.next();
        assertThat(result, notNullValue());
        assertThat(new TSONObject(result).get("value"), notNullValue());
        assertThat(new TSONObject(result).get("value"), is(true));

        assertThat(select.hasNext(), is(false));
    }

    @Test
    public void selectStringTest() {
        Iterator<String> select = dual.select("\"abc\"", null);

        assertThat(select, notNullValue());
        assertThat(select.hasNext(), is(true));

        String result = select.next();
        assertThat(result, notNullValue());
        assertThat(new TSONObject(result).get("value"), notNullValue());
        assertThat(new TSONObject(result).get("value"), is("abc"));

        assertThat(select.hasNext(), is(false));
    }

    @Test
    public void selectString2Test() {
        Iterator<String> select = dual.select("'abc'", null);

        assertThat(select, notNullValue());
        assertThat(select.hasNext(), is(true));

        String result = select.next();
        assertThat(result, notNullValue());
        assertThat(new TSONObject(result).get("value"), notNullValue());
        assertThat(new TSONObject(result).get("value"), is("abc"));

        assertThat(select.hasNext(), is(false));
    }

    @Test
    public void selectNameTest() {
        Iterator<String> select = dual.select("name", null);

        assertThat(select, notNullValue());
        assertThat(select.hasNext(), is(true));

        String result = select.next();
        assertThat(result, notNullValue());
        assertThat(new TSONObject(result).get("name"), notNullValue());
        assertThat(new TSONObject(result).get("name"), is("DUAL"));

        assertThat(select.hasNext(), is(false));
    }

    @Test
    public void selectRandomTest() {
        Iterator<String> select = dual.select("random", null);

        assertThat(select, notNullValue());
        assertThat(select.hasNext(), is(true));

        String result = select.next();
        assertThat(result, notNullValue());
        assertThat(new TSONObject(result).has("random"), is(true));
        assertThat(new TSONObject(result).get("random"), instanceOf(Integer.class));

        assertThat(select.hasNext(), is(false));
    }

    @Test
    public void selectDateTest() {
        Iterator<String> select = dual.select("date", null);

        assertThat(select, notNullValue());
        assertThat(select.hasNext(), is(true));

        String result = select.next();
        assertThat(result, notNullValue());
        assertThat(new TSONObject(result).has("date"), is(true));
        assertThat(Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d+")
                           .matcher(new TSONObject(result).get("date").toString()).matches(), is(true));

        assertThat(select.hasNext(), is(false));
    }

    @Test(expected = IllegalStateException.class)
    public void selectWithWhereTest() {
        dual.select("*", "1=1");
    }

    @Test(expected = IllegalStateException.class)
    public void insertTest() {
        dual.insert(Collections.singleton("{}"));
    }

    @Test(expected = IllegalStateException.class)
    public void deleteTest() {
        dual.delete(null);
    }

    @Test(expected = IllegalStateException.class)
    public void updateTest() {
        dual.update(new String[]{"test"}, new String[]{"test"}, null);
    }
}
