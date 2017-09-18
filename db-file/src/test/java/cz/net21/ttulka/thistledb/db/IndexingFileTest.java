package cz.net21.ttulka.thistledb.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author ttulka
 */
public class IndexingFileTest {

    private IndexingFile indexingFile;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void createDataSource() throws IOException {
        Path tempPath = temp.newFolder().toPath().resolve("test");
        indexingFile = new IndexingFile(tempPath, 0);
    }

    @Test
    public void basicTest() {
        indexingFile.create("person.name");

        indexingFile.insert("person.name", "John", 123L);

        Set<Long> positions1 = indexingFile.positions("person.name", "John");
        assertThat(positions1, contains(123L));

        indexingFile.insert("person.name", "John", 456L);

        Set<Long> positions2 = indexingFile.positions("person.name", "John");
        assertThat(positions2, containsInAnyOrder(123L, 456L));

        Set<Long> positions3 = indexingFile.positions("person.name", "Peter");
        assertThat(positions3.isEmpty(), is(true));

        indexingFile.insert("person.name", "Peter", 789L);

        Set<Long> positions4 = indexingFile.positions("person.name", "Peter");
        assertThat(positions4, contains(789L));

        indexingFile.delete("person.name", "John", 456L);

        Set<Long> positions5 = indexingFile.positions("person.name", "John");
        assertThat(positions5, contains(123L));

        indexingFile.drop("person.name");

        Set<Long> positions6 = indexingFile.positions("person.name", "Peter");
        assertThat(positions6, is(nullValue()));
    }

    @Test
    public void existsTest() {
        boolean exists1 = indexingFile.exists("abc");
        assertThat(exists1, is(false));
    }

    @Test
    public void createTest() {
        boolean create1 = indexingFile.create("abc");
        assertThat(create1, is(true));

        boolean exists1 = indexingFile.exists("abc");
        assertThat(exists1, is(true));

        boolean create2 = indexingFile.create("abc");
        assertThat(create2, is(false));

        boolean exists2 = indexingFile.exists("abc");
        assertThat(exists2, is(true));
    }

    @Test
    public void dropTest() {
        indexingFile.create("abc");
        boolean exists1 = indexingFile.exists("abc");
        assertThat(exists1, is(true));

        indexingFile.drop("abc");
        boolean exists2 = indexingFile.exists("abc");
        assertThat(exists2, is(false));
    }

    @Test
    public void dropAllTest() {
        indexingFile.create("index1");
        indexingFile.create("index2");

        boolean exists1 = indexingFile.exists("index1");
        assertThat(exists1, is(true));

        boolean exists2 = indexingFile.exists("index2");
        assertThat(exists2, is(true));

        indexingFile.dropAll();

        boolean exists1A = indexingFile.exists("index1");
        assertThat(exists1A, is(false));

        boolean exists2A = indexingFile.exists("index2");
        assertThat(exists2A, is(false));
    }

    @Test
    public void cleanUpDirectoryWalkTest() throws IOException {
        Path dir = temp.newFolder().toPath();

        Path test1 = Files.createDirectories(dir.resolve("test1").resolve("01"));
        Path test2 = Files.createDirectories(dir.resolve("test2").resolve("02"));
        Path test3 = Files.createDirectories(dir.resolve("test3").resolve("03"));

        Path index1 = Files.createFile(test1.resolve("index"));
        Path index2 = Files.createFile(test2.resolve("index"));
        Path index3 = Files.createFile(test3.resolve("index"));

        IndexingFile spyIndexingFile = spy(indexingFile);

        spyIndexingFile.cleanUpDirectory(dir);

        verify(spyIndexingFile).cleanUpIndex(index1);
        verify(spyIndexingFile).cleanUpIndex(index2);
        verify(spyIndexingFile).cleanUpIndex(index3);
    }

    @Test
    public void cleanUpIndexTest() throws IOException {
        Path index = temp.newFile().toPath();

        String lastRecord = "value" + IndexingFile.VALUE_SEPARATOR + "4" + IndexingFile.RECORD_SEPARATOR;

        // simulate modified index file
        try (BufferedWriter bw = Files.newBufferedWriter(index)) {
            bw.write(IndexingFile.RECORD_DELETED + "alue" + IndexingFile.VALUE_SEPARATOR + "1" + IndexingFile.RECORD_SEPARATOR);
            bw.write(IndexingFile.RECORD_DELETED + "alue" + IndexingFile.VALUE_SEPARATOR + "2" + IndexingFile.RECORD_SEPARATOR);
            bw.write(IndexingFile.RECORD_DELETED + "alue" + IndexingFile.VALUE_SEPARATOR + "3" + IndexingFile.RECORD_SEPARATOR);
            bw.write(lastRecord);
        }

        // clean it up
        indexingFile.cleanUpIndex(index);

        // read the content after the modification
        StringBuilder content = new StringBuilder();
        try (BufferedReader br = Files.newBufferedReader(index)) {
            int ch;
            while ((ch = br.read()) != -1) {
                content.append((char) ch);
            }
        }

        assertThat("Index file should be cleaned up.", content.toString(), is(lastRecord));
    }
}
