package cz.net21.ttulka.thistledb.server.db;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * @author ttulka
 */
public class IndexingTest {

    private Indexing indexing;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void createDataSource() throws IOException {
        Path tempPath = temp.newFolder().toPath().resolve("test");
        indexing = new Indexing(tempPath);
    }

    @Test
    public void basicTest() {
        indexing.create("person.name");

        indexing.insert("person.name", "John", 123L);

        Set<Long> positions1 = indexing.positions("person.name", "John");
        assertThat(positions1, contains(123L));

        indexing.insert("person.name", "John", 456L);

        Set<Long> positions2 = indexing.positions("person.name", "John");
        assertThat(positions2, containsInAnyOrder(123L, 456L));

        Set<Long> positions3 = indexing.positions("person.name", "Peter");
        assertThat(positions3.isEmpty(), is(true));

        indexing.insert("person.name", "Peter", 789L);

        Set<Long> positions4 = indexing.positions("person.name", "Peter");
        assertThat(positions4, contains(789L));

        indexing.delete("person.name", "John", 456L);

        Set<Long> positions5 = indexing.positions("person.name", "John");
        assertThat(positions5, contains(123L));

        indexing.drop("person.name");

        Set<Long> positions6 = indexing.positions("person.name", "Peter");
        assertThat(positions6.isEmpty(), is(true));
    }

    @Test
    public void existsTest() {
        boolean exists1 = indexing.exists("abc");
        assertThat(exists1, is(false));
    }

    @Test
    public void createTest() {
        boolean create1 = indexing.create("abc");
        assertThat(create1, is(true));

        boolean exists1 = indexing.exists("abc");
        assertThat(exists1, is(true));

        boolean create2 = indexing.create("abc");
        assertThat(create2, is(false));

        boolean exists2 = indexing.exists("abc");
        assertThat(exists2, is(true));
    }

    @Test
    public void dropTest() {
        indexing.create("abc");
        boolean exists1 = indexing.exists("abc");
        assertThat(exists1, is(true));

        indexing.drop("abc");
        boolean exists2 = indexing.exists("abc");
        assertThat(exists2, is(false));
    }

    @Test
    public void dropAllTest() {
        indexing.create("index1");
        indexing.create("index2");

        boolean exists1 = indexing.exists("index1");
        assertThat(exists1, is(true));

        boolean exists2 = indexing.exists("index2");
        assertThat(exists2, is(true));

        indexing.dropAll();

        boolean exists1A = indexing.exists("index1");
        assertThat(exists1A, is(false));

        boolean exists2A = indexing.exists("index2");
        assertThat(exists2A, is(false));
    }
}
