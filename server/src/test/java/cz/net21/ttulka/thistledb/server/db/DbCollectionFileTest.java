package cz.net21.ttulka.thistledb.server.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cz.net21.ttulka.thistledb.server.TestData;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class DbCollectionFileTest {

    private DbCollectionFile dbCollection;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void createDbCollection() throws IOException {
        Path tempFile = temp.newFile().toPath();

        dbCollection = new DbCollectionFile(tempFile);
    }

    @Test
    public void cleanUpCollectionFileTest() throws IOException {
        Path collectionPath = dbCollection.path;

        dbCollection.insert(Collections.singleton(TestData.JSON_BASIC));
        dbCollection.insert(Collections.singleton(TestData.JSON_PERSON));
        dbCollection.insert(Collections.singleton(TestData.JSON_PERSON));

        long originalSize = Files.size(collectionPath);

        dbCollection.delete("person.name = \"John\"");

        dbCollection.cleanUp();

        long afterCleanUpSize = Files.size(collectionPath);

        assertThat("Collection file size after cleanup should be less than before.", afterCleanUpSize < originalSize, is(true));
    }
}
