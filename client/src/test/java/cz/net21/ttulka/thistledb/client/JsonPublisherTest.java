package cz.net21.ttulka.thistledb.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONObject;
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
 * Created by ttulka
 */
@Test
public class JsonPublisherTest extends PublisherVerification<JSONObject> {

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

    public JsonPublisherTest() {
        super(new TestEnvironment());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Publisher<JSONObject> createPublisher(final long elements) {
        assert (elements <= maxElementsFromPublisher());

        Processor processor = mock(Processor.class);
        when(processor.getNextResult()).thenAnswer(new Answer<JSONObject>() {
            private int i;
            @Override
            public JSONObject answer(InvocationOnMock invocation) {
                return i < elements ? new JSONObject("{\"value\":\"" + i++ + "\"}") : null;
            }
        });
        return new JsonPublisher(processor);
    }

    @Override
    public Publisher<JSONObject> createFailedPublisher() {
        Processor processor = mock(Processor.class);
        when(processor.getNextResult()).thenThrow(new RuntimeException("Error state signal!"));
        return new JsonPublisher(processor);
    }

    @Override
    public long maxElementsFromPublisher() {
        return Integer.MAX_VALUE;
    }
}
