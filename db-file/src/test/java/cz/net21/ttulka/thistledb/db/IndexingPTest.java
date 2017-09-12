package cz.net21.ttulka.thistledb.db;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;

import org.junit.Before;
import org.junit.Ignore;
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
@Ignore
public class IndexingPTest {

    private static final int AMOUNT_OF_RECORDS = 1000;
    private static final int AMOUNT_OF_ROUNDS = 100;

    private static final int THRESHOLD = 3; // how many times must be performance better to pass the test

    private static final String TEST_COLLECTION_NAME = "test";

    private static final String BALLAST = "Frederick P. Brooks, Jr., is Kenan Professor of Computer Science at the University of North Carolina at Chapel Hill. He is best known as the 'father of the IBM System/360,' having served as project manager for its development and later as manager of the Operating System/360 software project during its design phase. For this work he, Bob Evans, and Erich Bloch were awarded the National Medal of Technology in 1985. Earlier, he was an architect of the IBM Stretch and Harvest computers. At Chapel Hill, Dr. Brooks founded the Department of Computer Science and chaired it from 1964 through 1984. He has served on the National Science Board and the Defense Science Board. His current teaching and research is in computer architecture, molecular graphics, and virtual environments.";

    private DataSourceFile dataSource;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        createDataSource();
        generateData();
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
        return "{\"root\":{\"id\":" + index + ",\"value\":\"" + value + "\",\"ballast\":\"" + BALLAST + "\"}}";
    }

    private Random random = new Random();

    private String generateValue() {
        return "V" + Math.abs(random.nextInt() % Math.round(Math.sqrt(AMOUNT_OF_RECORDS * 10)));
    }

    @Test
    public void performanceTest() {
        String needle = generateValue();

        long time1 = measure(needle, AMOUNT_OF_ROUNDS);
        System.out.println("PERFORMANCE TIME #1: " + time1 + " ms");

        dataSource.createIndex(TEST_COLLECTION_NAME, "root.value");

        long time2 = measure(needle, AMOUNT_OF_ROUNDS);
        System.out.println("PERFORMANCE TIME #2: " + time2 + " ms");

        assertThat("Indexed search must be at least " + THRESHOLD + " times quicker.",
                   time1 > time2 * THRESHOLD, is(true));
    }

    private long measure(String needle, int count) {
        long time = 0;
        for (int i = 0; i < count; i++) {
            time += measure(needle);
        }
        return time;
    }

    private long measure(String needle) {
        long start = System.currentTimeMillis();

        dataSource.select(TEST_COLLECTION_NAME,
                          "root.value",
                          "root.value = \"" + needle + "\"")
                .blockLast();

        return System.currentTimeMillis() - start;
    }
}
