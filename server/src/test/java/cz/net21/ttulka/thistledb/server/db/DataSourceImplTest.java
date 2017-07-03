package cz.net21.ttulka.thistledb.server.db;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cz.net21.ttulka.thistledb.server.TestData;
import cz.net21.ttulka.thistledb.tson.TSONObject;
import reactor.core.publisher.Flux;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

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
        Path tempFile = temp.newFolder().toPath();
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
    @Ignore
    public void createIndexTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        assertThat("New index should be created.", dataSource.createIndex(TEST_COLLECTION_NAME, "test.index"), is(true));
        assertThat("Existing index shouldn't be created again.", dataSource.createIndex(TEST_COLLECTION_NAME, "test.index"), is(false));
    }

    @Test
    @Ignore
    public void dropIndexTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);
        dataSource.createIndex(TEST_COLLECTION_NAME, "test.index");

        assertThat("Existing index should be dropped.", dataSource.dropIndex(TEST_COLLECTION_NAME, "test.index"), is(true));
        assertThat("Not-existing index shouldn't be dropped again.", dataSource.dropIndex(TEST_COLLECTION_NAME, "test.index"), is(false));
    }

    @Test(expected = DatabaseException.class)
    public void dropIndexOnNotExistingCollectionTest() {
        dataSource.dropIndex(TEST_COLLECTION_NAME, "test.index");
    }

    @Test
    public void selectAfterInsertTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_BASIC);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);

        Flux<TSONObject> stream = dataSource.select(TEST_COLLECTION_NAME, "*", null);

        List<String> results = new CopyOnWriteArrayList<>();
        stream.map(TSONObject::toString).subscribe(results::add);

        waitForSeconds(1);

        assertThat("Should return two records.", results.size(), is(2));
        assertThat("Should contain both items.", results, containsInAnyOrder(TestData.JSON_BASIC, TestData.JSON_PERSON));

        dataSource.dropCollection(TEST_COLLECTION_NAME);
    }

    private void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds);

        } catch (InterruptedException e) {
            // ignore
        }
    }
}
