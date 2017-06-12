package cz.net21.ttulka.thistledb.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

/**
 * Created by ttulka
 */
@RunWith(MockitoJUnitRunner.class)
public class JsonPublisherTest {

    private static final int ELEMENTS = 100;

    @Mock
    private QueryExecutor queryExecutor;

    @InjectMocks
    private JsonPublisher publisher;

    @Before
    public void setUp() {
        when(queryExecutor.getNextResult()).thenAnswer(new Answer<JSONObject>() {
            private int i;

            @Override
            public JSONObject answer(InvocationOnMock invocation) {
                String formattedNumber = String.format("%03d", i++);
                return i <= ELEMENTS ? new JSONObject("{\"value\":\"" + formattedNumber + "\"}") : null;
            }
        });
    }

    @Test
    public void subscribeTest() throws Exception {
        List<JSONObject> results = new ArrayList<>();
        publisher.serial().subscribe(json -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
            }
            results.add(json);
        });

        assertThat("It's too early to have all the elements.", results.size(), not(is(ELEMENTS)));

        Thread.sleep(ELEMENTS * 100);

        testOrderedResults(results);
    }

    @Test
    public void subscribeParallelTest() throws Exception {
        List<JSONObject> results = new CopyOnWriteArrayList<>();
        publisher.parallel().subscribe(json -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
            }
            results.add(json);
        });

        assertThat("It's too early to have all the elements.", results.size(), not(is(ELEMENTS)));

        Thread.sleep(ELEMENTS * 100);

        testUnorderedResults(results);
    }

    @Test
    public void awaitTest() throws Exception {
        long start = System.currentTimeMillis();

        List<JSONObject> results = new ArrayList<>();
        publisher.serial().subscribe(json -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
            }
            results.add(json);
        }).await();

        System.out.println("awaitTest: " + (System.currentTimeMillis() - start));

        testOrderedResults(results);
    }

    @Test
    public void awaitParallelTest() throws Exception {
        long start = System.currentTimeMillis();

        List<JSONObject> results = new CopyOnWriteArrayList<>();
        publisher.parallel().subscribe(json -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
            }
            results.add(json);
        }).await();

        System.out.println("awaitParallelTest: " + (System.currentTimeMillis() - start));

        testUnorderedResults(results);
    }

    private void testOrderedResults(List<JSONObject> results) {
        assertThat("We should have all the elements.", results.size(), is(ELEMENTS));

        IntStream.range(0, ELEMENTS).forEach(i -> {
            String formattedNumber = String.format("%03d", i);
            JSONObject json = new JSONObject("{\"value\":\"" + formattedNumber + "\"}");
            assertThat("The elements must be ordered.", results.get(i).toString(), is(json.toString()));
        });
    }

    private void testUnorderedResults(List<JSONObject> results) {
        assertThat("We should have all the elements.", results.size(), is(ELEMENTS));

        Collections.sort(results, Comparator.comparing(JSONObject::toString));

        IntStream.range(0, ELEMENTS).forEach(i -> {
            String formattedNumber = String.format("%03d", i);
            JSONObject json = new JSONObject("{\"value\":\"" + formattedNumber + "\"}");
            assertThat("The elements must be ordered.", results.get(i).toString(), is(json.toString()));
        });
    }
}
