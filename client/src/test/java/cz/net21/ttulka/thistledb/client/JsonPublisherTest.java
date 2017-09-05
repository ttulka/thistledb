package cz.net21.ttulka.thistledb.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author ttulka
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
        when(queryExecutor.getNextResult()).thenAnswer(new Answer<String>() {
            private int i;

            @Override
            public String answer(InvocationOnMock invocation) {
                String formattedNumber = String.format("%03d", i++);
                return i <= ELEMENTS ? new String("{\"value\":\"" + formattedNumber + "\"}") : null;
            }
        });
    }

    @Test
    public void subscribeTest() throws Exception {
        List<String> results = new ArrayList<>();
        publisher.serial().subscribe(json -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
            }
            results.add(json);
        });

        assertThat("It's too early to have all the elements.", results.size(), not(is(ELEMENTS)));

        Thread.sleep(ELEMENTS * 100);

        verify(queryExecutor).executeQuery();
        verify(queryExecutor, times(ELEMENTS + 1)).getNextResult(); // the last returns null
        verifyNoMoreInteractions(queryExecutor);

        testOrderedResults(results);
    }

    @Test
    public void subscribeParallelTest() throws Exception {
        List<String> results = new CopyOnWriteArrayList<>();
        publisher.parallel().subscribe(json -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
            }
            results.add(json);
        });

        assertThat("It's too early to have all the elements.", results.size(), not(is(ELEMENTS)));

        Thread.sleep(ELEMENTS * 100);

        verify(queryExecutor).executeQuery();
        verify(queryExecutor, times(ELEMENTS + 1)).getNextResult(); // the last returns null
        verifyNoMoreInteractions(queryExecutor);

        testUnorderedResults(results);
    }

    @Test
    public void awaitTest() throws Exception {
        long start = System.currentTimeMillis();

        List<String> results = new ArrayList<>();
        publisher.serial().subscribe(json -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
            }
            results.add(json);
        }).await();

        System.out.println("awaitTest: " + (System.currentTimeMillis() - start));

        verify(queryExecutor).executeQuery();
        verify(queryExecutor, times(ELEMENTS + 1)).getNextResult(); // the last returns null
        verifyNoMoreInteractions(queryExecutor);

        testOrderedResults(results);
    }

    @Test
    public void awaitParallelTest() throws Exception {
        long start = System.currentTimeMillis();

        List<String> results = new CopyOnWriteArrayList<>();
        publisher.parallel().subscribe(json -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
            }
            results.add(json);
        }).await();

        System.out.println("awaitParallelTest: " + (System.currentTimeMillis() - start));

        verify(queryExecutor).executeQuery();
        verify(queryExecutor, times(ELEMENTS + 1)).getNextResult(); // the last returns null
        verifyNoMoreInteractions(queryExecutor);

        testUnorderedResults(results);
    }

    @Test
    public void moreSubscribersTest() {
        long start = System.currentTimeMillis();

        List<String> results1 = new CopyOnWriteArrayList<>();
        List<String> results2 = new CopyOnWriteArrayList<>();
        publisher
                .subscribe(json -> {
                    try {
                        Thread.sleep(new Random().nextInt(100));
                    } catch (InterruptedException e) {
                    }
                    results1.add(json);
                })
                .subscribe(json -> {
                    try {
                        Thread.sleep(new Random().nextInt(100));
                    } catch (InterruptedException e) {
                    }
                    results2.add(json);
                })
                .await();

        System.out.println("moreSubscribersTest: " + (System.currentTimeMillis() - start));

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
        }

        verify(queryExecutor).executeQuery();
        verify(queryExecutor, times(ELEMENTS + 2)).getNextResult(); // the last request return null
        verifyNoMoreInteractions(queryExecutor);

        List<String> results = new ArrayList<>();
        results.addAll(results1);
        results.addAll(results2);

        testUnorderedResults(results);
    }

    @Test
    public void moreSubscribersParallelTest() {
        long start = System.currentTimeMillis();

        List<String> results1 = new CopyOnWriteArrayList<>();
        List<String> results2 = new CopyOnWriteArrayList<>();
        publisher.parallel()
                .subscribe(json -> {
                    try {
                        Thread.sleep(new Random().nextInt(100));
                    } catch (InterruptedException e) {
                    }
                    results1.add(json);
                })
                .subscribe(json -> {
                    try {
                        Thread.sleep(new Random().nextInt(100));
                    } catch (InterruptedException e) {
                    }
                    results2.add(json);
                })
                .await();

        System.out.println("moreSubscribersParallelTest: " + (System.currentTimeMillis() - start));

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
        }

        verify(queryExecutor).executeQuery();
        verify(queryExecutor, times(ELEMENTS + 2)).getNextResult(); // the last request return null
        verifyNoMoreInteractions(queryExecutor);

        List<String> results = new ArrayList<>();
        results.addAll(results1);
        results.addAll(results2);

        testUnorderedResults(results);
    }

    private void testOrderedResults(List<String> results) {
        assertThat("We should have all the elements.", results.size(), is(ELEMENTS));

        IntStream.range(0, ELEMENTS).forEach(i -> {
            String formattedNumber = String.format("%03d", i);
            String json = "{\"value\":\"" + formattedNumber + "\"}";
            assertThat("The elements must be ordered.", results.get(i), is(json));
        });
    }

    private void testUnorderedResults(List<String> results) {
        assertThat("We should have all the elements.", results.size(), is(ELEMENTS));

        Collections.sort(results);

        IntStream.range(0, ELEMENTS).forEach(i -> {
            String formattedNumber = String.format("%03d", i);
            String json = "{\"value\":\"" + formattedNumber + "\"}";
            assertThat("The elements must be ordered.", results.get(i), is(json));
        });
    }
}
