package cz.net21.ttulka.thistledb.client.reactive;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.json.JSONObject;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
        AtomicInteger i = new AtomicInteger(0);
        return new JsonPublisher(
                () -> {
                },
                () -> i.get() < elements ? new JSONObject("{\"v\":\"" + i.getAndIncrement() + "\"}") : null
        );
    }

    @Override
    public Publisher<JSONObject> createFailedPublisher() {
        return new JsonPublisher(() -> {        },
                                 () -> {
                                     throw new RuntimeException("Error state signal!");
                                 });
    }

    @Override
    public long maxElementsFromPublisher() {
        return Integer.MAX_VALUE;
    }
}
