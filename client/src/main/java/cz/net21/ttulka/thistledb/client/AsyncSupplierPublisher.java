package cz.net21.ttulka.thistledb.client;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * AsyncSupplierPublisher uses a supplier with only one 'get()' method for supplying values
 * from unknown streams like files or sockets.
 * The supplier informs about the end of stream by returning a 'null' value.
 */
class AsyncSupplierPublisher<T> extends AsyncIterablePublisher<T> {
    public AsyncSupplierPublisher(final Supplier<T> supplier, final Executor executor) {
        super(() -> new Iterator<T>() {
            private T elem = supplier.get();

            @Override
            public boolean hasNext() {
                return elem != null;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    return Collections.<T>emptyList().iterator().next();
                } else {
                    T prev = elem;
                    elem = supplier.get();
                    return prev;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }, executor);
    }
}