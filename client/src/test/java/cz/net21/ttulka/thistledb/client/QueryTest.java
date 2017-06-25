package cz.net21.ttulka.thistledb.client;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * Created by ttulka
 */
public class QueryTest {

    @Test
    public void selectTest() {
        Query q = Query.builder().select("test").build();
        assertThat(q.getNativeQuery(), is("SELECT * FROM test"));

        q = Query.builder().select("test", new String[]{"a1"}).build();
        assertThat(q.getNativeQuery(), is("SELECT a1 FROM test"));

        q = Query.builder().select("test").where("1", "1").build();
        assertThat(q.getNativeQuery(), is("SELECT * FROM test WHERE 1='1'"));

        q = Query.builder().select("test").where("1", "1").and("2", "2").build();
        assertThat(q.getNativeQuery(), is("SELECT * FROM test WHERE 1='1' AND 2='2'"));

        q = Query.builder().select("test").where("1", "1").and("2", "2").or("3", "3").build();
        assertThat(q.getNativeQuery(), is("SELECT * FROM test WHERE 1='1' AND 2='2' OR 3='3'"));
    }

    // TODO
}
