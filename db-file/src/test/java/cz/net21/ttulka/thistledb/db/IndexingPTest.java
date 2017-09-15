package cz.net21.ttulka.thistledb.db;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Performance Test.
 *
 * @author ttulka
 */
public class IndexingPTest {

    private static final int AMOUNT_OF_RECORDS = 10_000;
    private static final int AMOUNT_OF_ROUNDS = 100;

    private static final int THRESHOLD = String.valueOf(AMOUNT_OF_RECORDS).length() + 1; // how many times must be performance better to pass the test

    private static final String TEST_COLLECTION_NAME = "test";

    private static final String JUNK = "Frederick P. Brooks, Jr., is Kenan Professor of Computer Science at the University of North Carolina at Chapel Hill. He is best known as the 'father of the IBM System/360', having served as project manager for its development and later as manager of the Operating System/360 software project during its design phase. For this work he, Bob Evans, and Erich Bloch were awarded the National Medal of Technology in 1985. Earlier, he was an architect of the IBM Stretch and Harvest computers. At Chapel Hill, Dr. Brooks founded the Department of Computer Science and chaired it from 1964 through 1984. He has served on the National Science Board and the Defense Science Board. His current teaching and research is in computer architecture, molecular graphics, and virtual environments. To my surprise and delight, The Mythical Man-Month continues to be popular after 20 years. Over 250,000 copies are in print. People often ask which of the opinions and recommendations.";

    private DataSourceFile dataSource;

    private String[] needles = new String[AMOUNT_OF_ROUNDS];

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        createDataSource();

        long start = System.currentTimeMillis();
        generateData();

        System.out.println("GENERATION TIME: " + (System.currentTimeMillis() - start) + " ms");
    }

    private void createDataSource() throws IOException {
        Path tempFile = temp.newFolder().toPath();
        dataSource = new DataSourceFile(tempFile);
    }

    private void generateData() {
        dataSource.createCollection(TEST_COLLECTION_NAME);

        for (int i = 0; i < AMOUNT_OF_RECORDS; i++) {
            dataSource.insert(TEST_COLLECTION_NAME, generateJson(i));
        }
    }

    private String generateJson(int index) {
        String value = generateValue();

        if (AMOUNT_OF_RECORDS - index <= AMOUNT_OF_ROUNDS) {
            needles[AMOUNT_OF_RECORDS - index - 1] = value;
        }
        return "{\"root\":{\"id\":" + index + ",\"value\":\"" + value + "\",\"ballast\":\"" + JUNK + "\"}}";
    }

    private Random random = new Random();

    private String generateValue() {
        return "V" + Math.abs(random.nextInt() % Math.round(Math.sqrt(AMOUNT_OF_RECORDS * 10)));
    }

    @Test
    public void performanceTest() {
        long time1 = measure(needles, AMOUNT_OF_ROUNDS);
        System.out.println("PERFORMANCE TIME (NO INDEXING): " + time1 + " ms");

        long indexingTime = index();
        System.out.println("INDEXING TIME: " + indexingTime + " ms");

        long time2 = measure(needles, AMOUNT_OF_ROUNDS);
        System.out.println("PERFORMANCE TIME  (INDEXING): " + time2 + " ms");

        long cleanUp = cleanUpIndexing();
        System.out.println("INDEXING CLEAN UP TIME: " + cleanUp + " ms");

        long time3 = measure(needles, AMOUNT_OF_ROUNDS);
        System.out.println("PERFORMANCE TIME  (INDEXING CLEANED UP): " + time3 + " ms");

        assertThat("Indexed search must be at least " + THRESHOLD + " times quicker.",
                   time1 > time2 * THRESHOLD, is(true));
        assertThat("Indexed search must be at least " + THRESHOLD + " times quicker.",
                   time1 > time3 * THRESHOLD, is(true));
        assertThat("Indexed search after clean up must be almost same than before.",
                   time3 <= time2 + 10, is(true));

    }

    private long index() {
        long start = System.currentTimeMillis();

        dataSource.createIndex(TEST_COLLECTION_NAME, "root.value");

        return System.currentTimeMillis() - start;
    }

    private long cleanUpIndexing() {
        long start = System.currentTimeMillis();

        DbCollectionFile collection = (DbCollectionFile) dataSource.getCollection(TEST_COLLECTION_NAME);
        collection.indexing.cleanUp();

        return System.currentTimeMillis() - start;
    }

    private long measure(String[] needles, int count) {
        long time = 0;
        for (int i = 0; i < count; i++) {
            time += measure(needles[i]);
        }
        return time / count;
    }

    private long measure(String needle) {
        long start = System.currentTimeMillis();

        String found = dataSource.select(TEST_COLLECTION_NAME,
                                         "root.value",
                                         "root.value = \"" + needle + "\"")
                .blockLast();

        assertThat("Needle should be found.", found, is("{\"value\":\"" + needle + "\"}"));

        return System.currentTimeMillis() - start;
    }
}
