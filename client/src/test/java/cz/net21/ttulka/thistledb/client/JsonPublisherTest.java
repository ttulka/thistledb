package cz.net21.ttulka.thistledb.client;

import java.util.ArrayList;
import java.util.List;
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
                return i < ELEMENTS ? new JSONObject("{\"value\":\"" + i++ + "\"}") : null;
            }
        });
    }

    @Test
    public void subscribeTest() throws Exception {
        List<JSONObject> results = new ArrayList<>();
        publisher.subscribe(json -> {
            results.add(json);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        });

        assertThat("It's too early to have all the elements.", results.size(), not(is(ELEMENTS)));

        Thread.sleep(ELEMENTS * 100);

        testResults(results);
    }

    @Test
    public void awaitTest() throws Exception {
        List<JSONObject> results = new ArrayList<>();
        publisher.subscribe(json -> {
            results.add(json);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }).await();

        testResults(results);
    }

    private void testResults(List<JSONObject> results) {
        assertThat("We should have all the elements.", results.size(), is(ELEMENTS));

        IntStream.range(0, ELEMENTS).forEach(i -> {
            JSONObject json = new JSONObject("{\"value\":\"" + i + "\"}");
            assertThat("The elements must be ordered.", results.get(i).toString(), is(json.toString()));
        });
    }
}
