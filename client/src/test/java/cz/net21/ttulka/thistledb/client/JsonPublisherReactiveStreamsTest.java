package cz.net21.ttulka.thistledb.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author ttulka
 */
@Test
public class JsonPublisherReactiveStreamsTest extends PublisherVerification<String> {

    private ExecutorService executorService;

    @BeforeClass
    void before() {
        executorService = Executors.newFixedThreadPool(4);
    }

    @AfterClass
    void after() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public JsonPublisherReactiveStreamsTest() {
        super(new TestEnvironment());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Publisher<String> createPublisher(final long elements) {
        assert (elements <= maxElementsFromPublisher());

        QueryExecutor queryExecutor = mock(QueryExecutor.class);
        when(queryExecutor.getNextResult()).thenAnswer(new Answer<String>() {
            private int i;

            @Override
            public String answer(InvocationOnMock invocation) {
                return i < elements ? "{\"value\":\"" + i++ + "\"}" : null;
            }
        });
        return new JsonPublisher(queryExecutor);
    }

    @Override
    public Publisher<String> createFailedPublisher() {
        QueryExecutor queryExecutor = mock(QueryExecutor.class);
        when(queryExecutor.getNextResult()).thenThrow(new RuntimeException("Error state signal!"));
        return new JsonPublisher(queryExecutor);
    }

    @Override
    public long maxElementsFromPublisher() {
        return Integer.MAX_VALUE;
    }
}
