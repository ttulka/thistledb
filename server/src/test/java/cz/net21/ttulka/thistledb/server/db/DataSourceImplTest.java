package cz.net21.ttulka.thistledb.server.db;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class DataSourceImplTest {

    private static final String TEST_COLLECTION_NAME = "test";

    // TODO

    private DataSource dataSource;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void createDataSource() throws IOException {
        Path tempFile = temp.newFile().toPath();
        dataSource = new DataSourceImpl(tempFile);
    }

    @Test
    public void createCollectionTest() {
        assertThat("New collection should be created.", dataSource.createCollection(TEST_COLLECTION_NAME), is(true));
        assertThat("Existing collection shouldn't be created again.", dataSource.createCollection(TEST_COLLECTION_NAME), is(false));
    }

    @Test
    public void dropCollectionTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        assertThat("Existing collection should be dropped.", dataSource.dropCollection(TEST_COLLECTION_NAME), is(true));
        assertThat("Not-existing collection shouldn't be dropped again.", dataSource.dropCollection(TEST_COLLECTION_NAME), is(false));
    }

    @Test(expected = DatabaseException.class)
    public void createIndexOnNotExistingCollectionTest() {
        dataSource.createIndex(TEST_COLLECTION_NAME, "test.index");
    }

    @Test
    public void createIndexTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        assertThat("New index should be created.", dataSource.createIndex(TEST_COLLECTION_NAME, "test.index"), is(true));
        assertThat("Existing index shouldn't be created again.", dataSource.createIndex(TEST_COLLECTION_NAME, "test.index"), is(false));
    }

    @Test(expected = DatabaseException.class)
    public void dropIndexOnNotExistingCollectionTest() {
        dataSource.dropIndex(TEST_COLLECTION_NAME, "test.index");
    }

    @Test
    public void dropIndexTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);
        dataSource.createIndex(TEST_COLLECTION_NAME, "test.index");

        assertThat("Existing index should be dropped.", dataSource.dropIndex(TEST_COLLECTION_NAME, "test.index"), is(true));
        assertThat("Not-existing index shouldn't be dropped again.", dataSource.dropIndex(TEST_COLLECTION_NAME, "test.index"), is(false));
    }
}
