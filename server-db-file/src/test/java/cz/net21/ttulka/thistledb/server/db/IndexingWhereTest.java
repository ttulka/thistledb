package cz.net21.ttulka.thistledb.server.db;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

/**
 * @author ttulka
 */
@RunWith(MockitoJUnitRunner.class)
public class IndexingWhereTest {

    @Mock
    private Indexing indexing;

    @Before
    public void setUp() {
        when(indexing.exists(eq("person.surname"))).thenReturn(true);
        when(indexing.positions(eq("person.surname"), eq("Smith"))).thenReturn(Collections.singleton(123L));
    }

    @Test
    public void basicTest() {
        Where where = Where.create("person.surname='Smith'");

        IndexingWhere indexingWhere = new IndexingWhere(where, indexing);

        assertThat(indexingWhere.isIndexed(), is(true));
        assertThat(indexingWhere.nextPosition(), is(123L));
        assertThat(indexingWhere.nextPosition(), is(-1L));
    }

    @Test(expected = IllegalStateException.class)
    public void wrongTest() {
        Where where = Where.create("person.name='John'");

        IndexingWhere indexingWhere = new IndexingWhere(where, indexing);

        assertThat(indexingWhere.isIndexed(), is(false));
        indexingWhere.nextPosition();   // IllegalStateException
    }

    @Test
    public void basicANDTest() {
        Where where = Where.create("person.surname='Smith' AND person.name='John'");

        IndexingWhere indexingWhere = new IndexingWhere(where, indexing);

        assertThat(indexingWhere.isIndexed(), is(true));
        assertThat(indexingWhere.nextPosition(), is(123L));
        assertThat(indexingWhere.nextPosition(), is(-1L));
    }

    @Test
    public void basicAND2Test() {
        Where where = Where.create("person.surname='Smith' AND person.name='John' AND person.age=42");

        IndexingWhere indexingWhere = new IndexingWhere(where, indexing);

        assertThat(indexingWhere.isIndexed(), is(true));
        assertThat(indexingWhere.nextPosition(), is(123L));
        assertThat(indexingWhere.nextPosition(), is(-1L));
    }

    @Test
    public void basicORTest() {
        when(indexing.exists(eq("person.name"))).thenReturn(true);
        when(indexing.positions(eq("person.name"), eq("John"))).thenReturn(Collections.singleton(456L));

        Where where = Where.create("person.surname='Smith' OR person.name='John'");

        IndexingWhere indexingWhere = new IndexingWhere(where, indexing);

        assertThat(indexingWhere.isIndexed(), is(true));
        assertThat(indexingWhere.nextPosition(), anyOf(is(123L), is(456L)));
        assertThat(indexingWhere.nextPosition(), anyOf(is(123L), is(456L)));
        assertThat(indexingWhere.nextPosition(), is(-1L));
    }

    @Test(expected = IllegalStateException.class)
    public void wrongORTest() {
        Where where = Where.create("person.surname='Smith' OR person.name='John'");

        IndexingWhere indexingWhere = new IndexingWhere(where, indexing);

        assertThat(indexingWhere.isIndexed(), is(false));
        indexingWhere.nextPosition();   // IllegalStateException
    }

    @Test
    public void basicAND_ORTest() {
        when(indexing.exists(eq("person.name"))).thenReturn(true);
        when(indexing.positions(eq("person.name"), eq("John"))).thenReturn(Collections.singleton(456L));

        Where where = Where.create("person.surname='Smith' OR person.name='John' AND person.age=42");

        IndexingWhere indexingWhere = new IndexingWhere(where, indexing);

        assertThat(indexingWhere.isIndexed(), is(true));
        assertThat(indexingWhere.nextPosition(), anyOf(is(123L), is(456L)));
        assertThat(indexingWhere.nextPosition(), anyOf(is(123L), is(456L)));
        assertThat(indexingWhere.nextPosition(), is(-1L));
    }

    @Test
    public void wrongAND_ORTest() {
        when(indexing.exists(eq("person.name"))).thenReturn(true);
        when(indexing.positions(eq("person.name"), eq("John"))).thenReturn(Collections.singleton(456L));

        // second AND part is wrong because not all OR parts are indexed (person.age)
        Where where = Where.create("person.surname='Smith' AND person.name='John' OR person.age=42");

        IndexingWhere indexingWhere = new IndexingWhere(where, indexing);

        assertThat(indexingWhere.isIndexed(), is(true));
        assertThat(indexingWhere.nextPosition(), is(123L));
        assertThat(indexingWhere.nextPosition(), is(-1L));
    }
}
