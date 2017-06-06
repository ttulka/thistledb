package cz.net21.ttulka.thistledb.server;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class ServerTest {

    @Test
    public void startServerTest() throws InterruptedException {
        Server server = new Server();

        assertThat("Server doesn't listen before been started.", server.listening(), is(false));

        server.startAndWait(500);

        assertThat("Server listens after been started.", server.listening(), is(true));

        server.stop();

        assertThat("Server doesn't listen after been stopped.", server.listening(), is(false));
    }
}
