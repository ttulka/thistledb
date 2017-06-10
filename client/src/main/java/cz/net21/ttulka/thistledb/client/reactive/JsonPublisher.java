package cz.net21.ttulka.thistledb.client.reactive;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.json.JSONObject;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import cz.net21.ttulka.thistledb.client.ClientException;

/**
 * Created by ttulka
 * <p>
 * Reactive JSON publisher.
 */
public class JsonPublisher implements Publisher<JSONObject> {

    //public static final JSONObject EMPTY = new JSONObject();

    private final Publisher<JSONObject> publisher;

    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch finished = new CountDownLatch(1);

    // TODO put init and next to an object - QueryExecutor

    /**
     * @throws ClientException when something goes wrong
     */
    JsonPublisher(Runnable init, Supplier<JSONObject> next) {
        this.publisher = new AsyncStreamPublisher<>(
                new Supplier<JSONObject>() {
                    {
                        System.out.println("started.countDown();");
                        // start a socket initialization
                        started.countDown();
                    }

                    @Override
                    public JSONObject get() {
                        JSONObject json = next.get();
                        return json != null ? json : null;
                    }
                },
                Executors.newCachedThreadPool()
        );
        init(init, next);
    }

    private void init(Runnable init, Supplier<JSONObject> next) {
        new Thread(() -> {
            try {
                System.out.println("started.await();");
                started.await();

                System.out.println("init.run();");
                init.run();

            } catch (InterruptedException e) {
                throw new ClientException("Publisher was interrupted.", e);
            } catch (Exception e) {
                throw new ClientException("Exception by publishing.", e);
            }
        }).start();
    }

    boolean parallel = false;

    /**
     * A parallel, we use the AsyncSubscriber, otherwise the SyncSubscriber
     */
    public final JsonPublisher parallel() {
        parallel = true;
        return this;
    }

    @Override
    public void subscribe(Subscriber<? super JSONObject> subscriber) {
        System.out.println("subscribe(Subscriber<? super JSONObject> subscriber) " + subscriber);
        if (subscriber == null) {
            throw null;
        }
        started.countDown();
        publisher.subscribe(subscriber);
    }

    /**
     * A convenient method.
     *
     * @param onNext
     */
    public void subscribe(Consumer<JSONObject> onNext) {
        Predicate<JSONObject> onNextWithEmptyTest = json -> {
            if (json != null) {
                onNext.accept(json);
                return true;
            } else {
                return false;
            }
        };

        Subscriber<JSONObject> subscriber;
        if (parallel) {
            subscriber = new AsyncSubscriber<JSONObject>(Executors.newCachedThreadPool()) {
                @Override
                protected boolean whenNext(JSONObject element) {
                    return onNextWithEmptyTest.test(element);
                }

                @Override
                protected void whenComplete() {
                    finished.getCount();
                }
            };
        } else {
            subscriber = new SyncSubscriber<JSONObject>() {
                @Override
                protected boolean foreach(JSONObject element) {
                    return onNextWithEmptyTest.test(element);
                }

                @Override
                protected void whenComplete() {
                    finished.getCount();
                }
            };
        }
        subscribe(subscriber);
    }

    /**
     * Wait for the publisher finished.
     *
     * @throws IllegalStateException when no subscription done yet
     */
    public void await() {
        if (started.getCount() == 1) {
            throw new IllegalStateException("No subscription done yet.");
        }
        System.out.println("await()");
        try {
            finished.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("await() finished");
    }
}
