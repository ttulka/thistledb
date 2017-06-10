package cz.net21.ttulka.thistledb.client.reactive;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Created by ttulka
 */
@Test // Must be here for TestNG to find and run this, do not remove
public class AsyncStreamPublisherTest extends PublisherVerification<Integer> {

    private ExecutorService e;

    @BeforeClass
    void before() {
        e = Executors.newFixedThreadPool(4);
    }

    @AfterClass
    void after() {
        if (e != null) {
            e.shutdown();
        }
    }

    public AsyncStreamPublisherTest() {
        super(new TestEnvironment());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Publisher<Integer> createPublisher(final long elements) {
        assert (elements <= maxElementsFromPublisher());
        return new AsyncStreamPublisher(new Supplier<Integer>() {
            private int at;

            @Override
            public Integer get() {
                return at < elements ? at++ : null;
            }
        }, e);
    }

    @Override
    public Publisher<Integer> createFailedPublisher() {
        return new AsyncStreamPublisher<>(new Supplier<Integer>() {
            @Override
            public Integer get() {
                throw new RuntimeException("Error state signal!");
            }
        }, e);
    }

    @Override
    public long maxElementsFromPublisher() {
        return Integer.MAX_VALUE;
    }
}