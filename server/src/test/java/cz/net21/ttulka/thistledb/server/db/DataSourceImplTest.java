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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
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
    @Ignore // TODO
    public void createIndexTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        assertThat("New index should be created.", dataSource.createIndex(TEST_COLLECTION_NAME, "test.index"), is(true));
        assertThat("Existing index shouldn't be created again.", dataSource.createIndex(TEST_COLLECTION_NAME, "test.index"), is(false));
    }

    @Test
    @Ignore // TODO
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
    public void selectTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_BASIC);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);

        Flux<TSONObject> stream = dataSource.select(TEST_COLLECTION_NAME, "*");

        List<String> results = new CopyOnWriteArrayList<>();
        stream.map(TSONObject::toString).subscribe(results::add);

        waitForSeconds(1);

        assertThat("Should return two records.", results.size(), is(2));
        assertThat("Should contain both items.", results, containsInAnyOrder(TestData.JSON_BASIC, TestData.JSON_PERSON));

        dataSource.dropCollection(TEST_COLLECTION_NAME);
    }

    @Test
    public void selectElementTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_BASIC);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);

        Flux<TSONObject> stream = dataSource.select(TEST_COLLECTION_NAME, "person.name");

        List<String> results = new CopyOnWriteArrayList<>();
        stream.map(TSONObject::toString).subscribe(results::add);

        waitForSeconds(1);

        assertThat("Should return two records.", results.size(), is(2));
        assertThat("Should contain both items.", results, containsInAnyOrder("{}", "{\"name\":\"John\"}"));

        dataSource.dropCollection(TEST_COLLECTION_NAME);
    }

    @Test
    public void selectWhereTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_BASIC);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);

        Flux<TSONObject> stream = dataSource.select(TEST_COLLECTION_NAME, "*", "person.name = \"John\"");

        List<String> results = new CopyOnWriteArrayList<>();
        stream.map(TSONObject::toString).subscribe(results::add);

        waitForSeconds(1);

        assertThat("Should return two records.", results.size(), is(2));
        assertThat("Should contain both items.", results, containsInAnyOrder(TestData.JSON_PERSON, TestData.JSON_PERSON));

        dataSource.dropCollection(TEST_COLLECTION_NAME);
    }

    @Test
    public void selectWhereUnsatisfiedableTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_BASIC);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);

        Flux<TSONObject> stream = dataSource.select(TEST_COLLECTION_NAME, "*", "person.name = \"XXX\"");

        List<String> results = new CopyOnWriteArrayList<>();
        stream.map(TSONObject::toString).subscribe(results::add);

        waitForSeconds(1);

        assertThat("Shouldn't return any records.", results.size(), is(0));

        dataSource.dropCollection(TEST_COLLECTION_NAME);
    }

    @Test
    public void deleteTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_BASIC);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);

        boolean deleted = dataSource.delete(TEST_COLLECTION_NAME);

        assertThat("Should be deleted.", deleted, is(true));

        Flux<TSONObject> stream = dataSource.select(TEST_COLLECTION_NAME, "*");

        List<String> results = new CopyOnWriteArrayList<>();
        stream.map(TSONObject::toString).subscribe(results::add);

        waitForSeconds(1);

        assertThat("Should contain no records.", results.size(), is(0));

        dataSource.dropCollection(TEST_COLLECTION_NAME);
    }

    @Test
    public void deleteWhereTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_BASIC);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);

        boolean deleted = dataSource.delete(TEST_COLLECTION_NAME, "person.name = \"John\"");

        assertThat("Should be deleted.", deleted, is(true));

        Flux<TSONObject> stream = dataSource.select(TEST_COLLECTION_NAME, "*");

        List<String> results = new CopyOnWriteArrayList<>();
        stream.map(TSONObject::toString).subscribe(results::add);

        waitForSeconds(1);

        assertThat("Should return one record.", results.size(), is(1));
        assertThat("Should contain basic json.", results, contains(TestData.JSON_BASIC));

        dataSource.dropCollection(TEST_COLLECTION_NAME);
    }

    @Test
    public void deleteWhereUnsatisfiedableTest() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_BASIC);
        dataSource.insert(TEST_COLLECTION_NAME, TestData.TSON_PERSON);

        boolean deleted = dataSource.delete(TEST_COLLECTION_NAME, "person.name = \"XXX\"");

        assertThat("Shouldn't be deleted.", deleted, is(false));

        Flux<TSONObject> stream = dataSource.select(TEST_COLLECTION_NAME, "*");

        List<String> results = new CopyOnWriteArrayList<>();
        stream.map(TSONObject::toString).subscribe(results::add);

        waitForSeconds(1);

        assertThat("Should return two records.", results.size(), is(2));
        assertThat("Should contain both items.", results, containsInAnyOrder(TestData.JSON_BASIC, TestData.JSON_PERSON));

        dataSource.dropCollection(TEST_COLLECTION_NAME);
    }

    private void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);

        } catch (InterruptedException e) {
            // ignore
        }
    }
}
