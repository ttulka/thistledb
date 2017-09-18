package cz.net21.ttulka.thistledb.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cz.net21.ttulka.thistledb.client.Client;
import cz.net21.ttulka.thistledb.client.JsonPublisher;
import cz.net21.ttulka.thistledb.client.Query;
import cz.net21.ttulka.thistledb.server.Server;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Client integration tests.
 *
 * @author ttulka
 */
public class ClientServerITest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private Server server;
    private Client client;

    @Before
    public void setUp() throws IOException {
        server = Server.builder().dataDir(temp.newFolder().toPath()).build();
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
    public void advanceSequenceTest() {
        String result;
        List<String> results;

        result = client.executeCommandBlocking("CREATE test");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("INSERT INTO test VALUES {\"name\":\"John\"},{\"name\":\"Peter\"}");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("CREATE INDEX name ON test");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("INSERT INTO test VALUES {\"name\":\"Jame\"}");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("UPDATE test SET name='James' WHERE name='Jame'");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("INSERT INTO test VALUES {\"name\":\"Bob\"}");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("INSERT INTO test VALUES {\"name\":\"Thomas\"}");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT * FROM test");
        assertThat(results, containsInAnyOrder("{\"name\":\"John\"}", "{\"name\":\"Peter\"}", "{\"name\":\"James\"}", "{\"name\":\"Bob\"}", "{\"name\":\"Thomas\"}"));

        results = client.executeQueryBlocking("SELECT * FROM test WHERE name = 'John'");
        assertThat(results, containsInAnyOrder("{\"name\":\"John\"}"));

        results = client.executeQueryBlocking("SELECT name FROM test WHERE name = 'John'");
        assertThat(results, containsInAnyOrder("{\"name\":\"John\"}"));

        results = client.executeQueryBlocking("SELECT xxx FROM test WHERE name = 'John'");
        assertThat(results, containsInAnyOrder("{}"));

        results = client.executeQueryBlocking("SELECT xxx FROM test WHERE name = 'XXX'");
        assertThat(results.isEmpty(), is(true));

        results = client.executeQueryBlocking("SELECT xxx FROM test WHERE xxx = 'XXX'");
        assertThat(results.isEmpty(), is(true));

        results = client.executeQueryBlocking("SELECT * FROM test WHERE name='John' OR name='Thomas'");
        assertThat(results, containsInAnyOrder("{\"name\":\"John\"}", "{\"name\":\"Thomas\"}"));

        results = client.executeQueryBlocking("SELECT name FROM test WHERE name='John' AND name='Thomas'");
        assertThat(results.isEmpty(), is(true));

        results = client.executeQueryBlocking("SELECT * FROM test WHERE name='XXX'");
        assertThat(results.isEmpty(), is(true));

        result = client.executeCommandBlocking("UPDATE test SET name='Johnny' WHERE name='John'");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("UPDATE test SET name='Jimmy' WHERE name='James'");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT * FROM test");
        assertThat(results, containsInAnyOrder("{\"name\":\"Johnny\"}", "{\"name\":\"Jimmy\"}", "{\"name\":\"Peter\"}", "{\"name\":\"Bob\"}", "{\"name\":\"Thomas\"}"));

        results = client.executeQueryBlocking("SELECT surname FROM test WHERE name = 'Jimmy'");
        assertThat(results, containsInAnyOrder("{}"));

        result = client.executeCommandBlocking("ALTER test ADD surname");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT surname FROM test WHERE name = 'Jimmy'");
        assertThat(results, containsInAnyOrder("{\"surname\":null}"));

        result = client.executeCommandBlocking("ALTER test ADD age");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("UPDATE test SET surname='Unknown'");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("UPDATE test SET age = 42");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT surname FROM test WHERE name = 'Thomas'");
        assertThat(results, containsInAnyOrder("{\"surname\":\"Unknown\"}"));

        results = client.executeQueryBlocking("SELECT age FROM test WHERE name = 'Thomas'");
        assertThat(results, containsInAnyOrder("{\"age\":42}"));

        result = client.executeCommandBlocking("UPDATE test SET surname='Hardy',age=33 WHERE name='Thomas'");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT surname FROM test WHERE name = 'Thomas'");
        assertThat(results, containsInAnyOrder("{\"surname\":\"Hardy\"}"));

        results = client.executeQueryBlocking("SELECT age FROM test WHERE name = 'Thomas'");
        assertThat(results, containsInAnyOrder("{\"age\":33}"));

        results = client.executeQueryBlocking("SELECT surname FROM test WHERE name = 'Bob'");
        assertThat(results, containsInAnyOrder("{\"surname\":\"Unknown\"}"));

        results = client.executeQueryBlocking("SELECT age FROM test WHERE name = 'Bob'");
        assertThat(results, containsInAnyOrder("{\"age\":42}"));

        results = client.executeQueryBlocking("SELECT name FROM test WHERE age < 42");
        assertThat(results, containsInAnyOrder("{\"name\":\"Thomas\"}"));

        result = client.executeCommandBlocking("ALTER test REMOVE age WHERE age >= 42");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT name FROM test WHERE age > 0");
        assertThat(results, containsInAnyOrder("{\"name\":\"Thomas\"}"));

        results = client.executeQueryBlocking("SELECT * FROM test");
        assertThat(results.size(), is(5));

        result = client.executeCommandBlocking("INSERT INTO test VALUES {'name':'Steven','surname':'Spoon'}");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT * FROM test");
        assertThat(results.size(), is(6));

        result = client.executeCommandBlocking("DELETE FROM test WHERE age=33 OR name LIKE 'j*'");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("DELETE FROM test WHERE name='Steven'");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT name FROM test");
        assertThat(results, containsInAnyOrder("{\"name\":\"Peter\"}", "{\"name\":\"Bob\"}"));

        results = client.executeQueryBlocking("SELECT surname FROM test");
        assertThat(results, containsInAnyOrder("{\"surname\":\"Unknown\"}", "{\"surname\":\"Unknown\"}"));

        result = client.executeCommandBlocking("UPDATE test SET name='X', surname='P' WHERE name='Peter'");
        assertThat(result, is("{\"status\":\"okay\"}"));

        result = client.executeCommandBlocking("UPDATE test SET name='X', surname='B' WHERE name='Bob'");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT name FROM test");
        assertThat(results, containsInAnyOrder("{\"name\":\"X\"}", "{\"name\":\"X\"}"));

        results = client.executeQueryBlocking("SELECT name FROM test WHERE name = 'X'");
        assertThat(results, containsInAnyOrder("{\"name\":\"X\"}", "{\"name\":\"X\"}"));

        results = client.executeQueryBlocking("SELECT surname FROM test");
        assertThat(results, containsInAnyOrder("{\"surname\":\"B\"}", "{\"surname\":\"P\"}"));

        result = client.executeCommandBlocking("DROP INDEX name ON test");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT name FROM test");
        assertThat(results, containsInAnyOrder("{\"name\":\"X\"}", "{\"name\":\"X\"}"));

        results = client.executeQueryBlocking("SELECT name FROM test WHERE name = 'X'");
        assertThat(results, containsInAnyOrder("{\"name\":\"X\"}", "{\"name\":\"X\"}"));

        results = client.executeQueryBlocking("SELECT surname FROM test");
        assertThat(results, containsInAnyOrder("{\"surname\":\"B\"}", "{\"surname\":\"P\"}"));

        result = client.executeCommandBlocking("DELETE FROM test");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT * FROM test");
        assertThat(results.isEmpty(), is(true));

        result = client.executeCommandBlocking("INSERT INTO test VALUES {'name':'John','surname':'Smith'}");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT name FROM test");
        assertThat(results, containsInAnyOrder("{\"name\":\"John\"}"));

        result = client.executeCommandBlocking("DROP test");
        assertThat(result, is("{\"status\":\"okay\"}"));

        results = client.executeQueryBlocking("SELECT * FROM test");
        assertThat(results.get(0), startsWith("{\"status\":\"error\""));
    }

    @Test
    public void parallelTest() {
        final int amountOfParallelClients = 100;

        client.executeCommandBlocking("CREATE test");
        client.executeCommandBlocking("CREATE INDEX name ON test");

        server.setMaxClientConnections(amountOfParallelClients + 2);

        ExecutorService executor = Executors.newCachedThreadPool();
        Runnable[] threads = new Runnable[amountOfParallelClients];

        IntStream.range(0, amountOfParallelClients).forEach(i -> {
            threads[i] = () -> {
                String name = Thread.currentThread().getName() + "@";

                try (Client client = new Client()) {
                    client.executeCommandBlocking("INSERT INTO test VALUES {'name':'" + name + "'}");

                    client.executeCommandBlocking("UPDATE test SET name='" + name + "-updated' WHERE name='" + name + "'");
                }
            };
        });

        Stream.of(threads).forEach(executor::execute);

        sleep(2000);

        executor.shutdown();
        while (!executor.isTerminated()) {
        }

        List<String> results = client.executeQueryBlocking("SELECT * FROM test WHERE name LIKE '*-updated'");
        assertThat(results.size(), is(amountOfParallelClients));

        client.executeCommandBlocking("DROP test");
    }

    @Test
    public void basicTest() {
        client.executeCommand("CREATE test", null);
        sleep(500);

        client.executeCommand("INSERT INTO test VALUES {\"v\":1},{\"v\":2}", null);
        sleep(500);

        client.executeCommand("UPDATE test SET v = 3 WHERE v = 2", null);
        sleep(500);

        JsonPublisher publisher = client.executeQuery("SELECT v FROM test WHERE v = 3");
        assertThat("Publisher shouldn't be null.", publisher, not(nullValue()));

        List<String> results = new ArrayList<>();
        publisher.subscribe(results::add);
        publisher.await();

        assertThat("There should be two results.", results, containsInAnyOrder("{\"v\":3}"));
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
