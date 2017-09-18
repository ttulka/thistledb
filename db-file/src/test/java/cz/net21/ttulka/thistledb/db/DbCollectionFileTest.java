package cz.net21.ttulka.thistledb.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cz.net21.ttulka.thistledb.TestData;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author ttulka
 */
public class DbCollectionFileTest {

    private DbCollectionFile dbCollection;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void createDbCollection() throws IOException {
        Path tempFile = temp.newFile().toPath();

        dbCollection = new DbCollectionFile(tempFile, 0);
    }

    @Test
    public void cleanUpCollectionFileTest() throws IOException {
        Path collectionPath = dbCollection.path;

        dbCollection.insert(Collections.singleton(TestData.JSON_BASIC));
        dbCollection.insert(Collections.singleton(TestData.JSON_PERSON));
        dbCollection.insert(Collections.singleton(TestData.JSON_PERSON));

        long originalSize = Files.size(collectionPath);

        dbCollection.createIndex("person.name");
        dbCollection.update(new String[]{"person.name"}, new String[]{"Peter"}, "person.name = \"John\"");
        dbCollection.delete("person.name = \"Peter\"");

        dbCollection.cleanUp();

        long afterCleanUpSize = Files.size(collectionPath);

        assertThat("Collection file size after cleanup should be less than before.", afterCleanUpSize < originalSize, is(true));
    }

    @Test
    public void indexingTest() {
        dbCollection.insert(Collections.singleton(TestData.JSON_BASIC));
        dbCollection.insert(Collections.singleton("{\"person\":{\"name\":\"Peter\",\"surname\":\"Jumble\"}}"));
        dbCollection.insert(Collections.singleton("{\"person\":{\"name\":\"Tomas\",\"surname\":\"Tulka\"}}"));
        dbCollection.insert(Collections.singleton("{\"person\":{\"name\":\"John\",\"surname\":\"Lennon\"}}"));

        Set<Long> positions = dbCollection.indexing.positions("person.name", "John");
        assertThat(positions, is(nullValue()));     // index not created yet

        dbCollection.createIndex("person.name");

        positions = dbCollection.indexing.positions("person.name", "John");
        assertThat(positions.size(), is(1));

        dbCollection.insert(Collections.singleton("{\"person\":{\"name\":\"Peter\",\"surname\":\"McDonald\"}}"));

        positions = dbCollection.indexing.positions("person.name", "John");
        assertThat(positions.size(), is(1));

        dbCollection.update(new String[]{"person.name"}, new String[]{"John"}, "person.surname=\"McDonald\"");

        positions = dbCollection.indexing.positions("person.name", "John");
        assertThat(positions.size(), is(2));    // John Lennon, John McDonald

        dbCollection.delete("person.surname=\"Lennon\"");

        positions = dbCollection.indexing.positions("person.name", "John");
        assertThat(positions.size(), is(1));    // only John McDonald
    }
}
