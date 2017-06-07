package cz.net21.ttulka.thistledb.client;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.json.JSONObject;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Created by ttulka
 * <p>
 * Reactive publisher.
 */
public class JsonPublisher implements Publisher<JSONObject> {

    private final Executor executor;

    public JsonPublisher() {
        executor = Executors.newCachedThreadPool();
    }

    @Override
    public void subscribe(Subscriber<? super JSONObject> subscriber) {
        // TODO
    }
}
