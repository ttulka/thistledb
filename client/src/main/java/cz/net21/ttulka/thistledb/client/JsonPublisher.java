package cz.net21.ttulka.thistledb.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.json.JSONObject;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Created by ttulka
 * <p>
 * Reactive JSON publisher.
 */
public class JsonPublisher implements Publisher<JSONObject> {

    private final Publisher<JSONObject> publisher;

    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch finished = new CountDownLatch(1);
    private final CountDownLatch initialized = new CountDownLatch(1);

    /**
     * @throws ClientException when something goes wrong
     */
    JsonPublisher(QueryExecutor queryExecutor) {
        this.publisher = createPublisher(() -> queryExecutor.getNextResult());
        init(() -> queryExecutor.executeQuery());
    }

    private Publisher<JSONObject> createPublisher(Supplier<JSONObject> supplier) {
        return new AsyncStreamPublisher<>(supplier, Executors.newCachedThreadPool());
    }

    private void init(Runnable init) {
        new Thread(() -> {
            try {
                started.await();
                init.run();
                initialized.countDown();

            } catch (InterruptedException e) {
                throw new ClientException("Publisher was interrupted.", e);
            } catch (Exception e) {
                throw new ClientException("Exception by publishing.", e);
            }
        }).start();
    }

    boolean parallel = false;

    /**
     * Changes the publisher to parallel processing.
     */
    public final JsonPublisher parallel() {
        parallel = true;
        return this;
    }

    /**
     * Changes the publisher to serial processing.
     */
    public final JsonPublisher serial() {
        parallel = false;
        return this;
    }

    /**
     * Subscribes a subscriber.
     * Recommended convenient alternative: {{@link #subscribe(Consumer)}}.
     *
     * @param subscriber the subscriber
     */
    @Override
    public void subscribe(Subscriber<? super JSONObject> subscriber) {
        // start initialization with the first subscription
        started.countDown();
        try {
            initialized.await();    // wait until is initialization finished
        } catch (InterruptedException e) {
            // ignore
        }
        publisher.subscribe(subscriber);
    }

    /**
     * Subscribes a consumer.
     *
     * @param onNext the consumer
     */
    public JsonPublisher subscribe(Consumer<JSONObject> onNext) {
        final ExecutorService executor = Executors.newCachedThreadPool();

        Subscriber<JSONObject> subscriber = new AsyncSubscriber<JSONObject>(Executors.newCachedThreadPool()) {
            @Override
            protected boolean whenNext(JSONObject json) {
                if (json != null) {
                    if (parallel) {
                        executor.execute(() -> onNext.accept(json));
                    } else {
                        onNext.accept(json);
                    }
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            protected void whenComplete() {
                if (parallel) {
                    executor.shutdown();
                    try {
                        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

                    } catch (InterruptedException e) {
                        throw new IllegalThreadStateException("Executor interrupted.");
                    }
                }
                finished.countDown();
            }
        };
        subscribe(subscriber);
        return this;
    }

    /**
     * Waits for the publisher finished publishing.
     *
     * @throws IllegalStateException when no subscription done yet
     */
    public void await() {
        if (started.getCount() == 1) {
            throw new IllegalStateException("No subscription done yet.");
        }
        try {
            finished.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
