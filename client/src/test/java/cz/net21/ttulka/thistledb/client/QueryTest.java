package cz.net21.ttulka.thistledb.client;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class QueryTest {

    @Test
    public void selectFromTest() {
        Query q = Query.builder().selectFrom("test").build();
        assertThat(q.getNativeQuery(), is("SELECT * FROM test"));

        q = Query.builder().selectFrom("test", new String[]{"a1"}).build();
        assertThat(q.getNativeQuery(), is("SELECT a1 FROM test"));

        q = Query.builder().selectFrom("test").where("1", "1").build();
        assertThat(q.getNativeQuery(), is("SELECT * FROM test WHERE 1='1'"));

        q = Query.builder().selectFrom("test").where("1", "1").and("2", "2").build();
        assertThat(q.getNativeQuery(), is("SELECT * FROM test WHERE 1='1' AND 2='2'"));

        q = Query.builder().selectFrom("test").where("1", "1").and("2", "2").or("3", "3").build();
        assertThat(q.getNativeQuery(), is("SELECT * FROM test WHERE 1='1' AND 2='2' OR 3='3'"));
    }

    @Test
    public void insertIntoTest() {
        Query q = Query.builder().insertInto("test", "{}").build();
        assertThat(q.getNativeQuery(), is("INSERT INTO test VALUES {}"));
    }

    @Test
    public void updateTest() {
        Query q = Query.builder().update("test").set("a", "1").build();
        assertThat(q.getNativeQuery(), is("UPDATE test SET a='1'"));

        q = Query.builder().update("test").set("a", "1").set("b", "2").build();
        assertThat(q.getNativeQuery(), is("UPDATE test SET a='1', b='2'"));
    }

    @Test
    public void deleteFromTest() {
        Query q = Query.builder().deleteFrom("test").build();
        assertThat(q.getNativeQuery(), is("DELETE FROM test"));

        q = Query.builder().deleteFrom("test").where("1", "1").build();
        assertThat(q.getNativeQuery(), is("DELETE FROM test WHERE 1='1'"));

        q = Query.builder().deleteFrom("test").where("1", "1").and("2", "2").build();
        assertThat(q.getNativeQuery(), is("DELETE FROM test WHERE 1='1' AND 2='2'"));

        q = Query.builder().deleteFrom("test").where("1", "1").and("2", "2").or("3", "3").build();
        assertThat(q.getNativeQuery(), is("DELETE FROM test WHERE 1='1' AND 2='2' OR 3='3'"));
    }

    @Test
    public void createCollectionTest() {
        Query q = Query.builder().createCollection("test").build();
        assertThat(q.getNativeQuery(), is("CREATE test"));
    }

    @Test
    public void dropCollectionTest() {
        Query q = Query.builder().dropCollection("test").build();
        assertThat(q.getNativeQuery(), is("DROP test"));
    }

    @Test
    public void createIndexTest() {
        Query q = Query.builder().createIndex("test", "abc").build();
        assertThat(q.getNativeQuery(), is("CREATE INDEX abc ON test"));
    }

    @Test
    public void dropIndexTest() {
        Query q = Query.builder().dropIndex("test", "abc").build();
        assertThat(q.getNativeQuery(), is("DROP INDEX abc ON test"));
    }
}
