package cz.net21.ttulka.thistledb.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cz.net21.ttulka.thistledb.server.Server;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Client integration tests.
 *
 * @author ttulka
 */
public class ClientITest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private Server server;
    private Client client;

    @Before
    public void setUp() throws IOException {
        server = new Server(temp.newFolder().toPath());
        server.start(5000);

        client = new Client();
    }

    @After
    public void tearDown() {
        client.close();

        server.stop(5000);
    }

    @Test
    public void testClientConnectionTest() {
        boolean test = client.test();
        assertThat("Test should be successful.", test, is(true));
    }

    @Test
    public void basicTest() {
        client.executeCommand("CREATE test", null);
        sleep(500);

        client.executeCommand("INSERT INTO test VALUES {\"v\":1},{\"v\":2}", null);
        sleep(500);

        client.executeCommandBlocking("UPDATE test SET v = 3 WHERE v = 2");
        sleep(500);

        JsonPublisher publisher = client.executeQuery("SELECT v FROM test");
        assertThat("Publisher shouldn't be null.", publisher, not(nullValue()));

        List<String> results = new ArrayList<>();
        publisher.subscribe(results::add);
        publisher.await();

        assertThat("There should be two results.", results, containsInAnyOrder("{\"v\":1}", "{\"v\":3}"));
    }

    @Test
    public void basicQueryTest() {
        Query createQuery = Query.builder().createCollection("test").build();
        client.executeCommand(createQuery, null);
        sleep(500);

        Query insertQuery = Query.builder().insertInto("test").values("{\"v\":1}").values("{\"v\":2}").build();
        client.executeCommand(insertQuery, null);
        sleep(500);

        Query updateQuery = Query.builder().update("test").set("v", 3).where("v", 2).build();
        client.executeCommand(updateQuery, null);
        sleep(500);

        Query selectQuery = Query.builder().selectFrom("test").build();
        JsonPublisher publisher = client.executeQuery(selectQuery);
        assertThat("Publisher shouldn't be null.", publisher, not(nullValue()));

        List<String> results = new ArrayList<>();
        publisher.subscribe(results::add);
        publisher.await();

        assertThat("There should be two results.", results, containsInAnyOrder("{\"v\":1}", "{\"v\":3}"));
    }

    @Test
    public void basicBlockingTest() {
        client.executeCommandBlocking("CREATE test");

        client.executeCommandBlocking("INSERT INTO test VALUES {\"v\":1},{\"v\":2}");

        client.executeCommandBlocking("UPDATE test SET v = 3 WHERE v = 2");

        List<String> results = client.executeQueryBlocking("SELECT v FROM test");

        assertThat("There should be two results.", results, containsInAnyOrder("{\"v\":1}", "{\"v\":3}"));
    }

    @Test
    public void basicBlockingQueryTest() {
        Query createQuery = Query.builder().createCollection("test").build();
        client.executeCommandBlocking(createQuery);

        Query insertQuery = Query.builder().insertInto("test").values("{\"v\":1}").values("{\"v\":2}").build();
        client.executeCommandBlocking(insertQuery);

        Query updateQuery = Query.builder().update("test").set("v", 3).where("v", 2).build();
        client.executeCommandBlocking(updateQuery);

        Query selectQuery = Query.builder().selectFrom("test").build();
        List<String> results = client.executeQueryBlocking(selectQuery);

        assertThat("There should be two results.", results, containsInAnyOrder("{\"v\":1}", "{\"v\":3}"));
    }

    private void sleep(int milsecs) {
        try {
            Thread.sleep(milsecs);

        } catch (InterruptedException ignore) {
        }
    }
}
