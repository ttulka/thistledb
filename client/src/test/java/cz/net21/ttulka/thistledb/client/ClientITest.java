package cz.net21.ttulka.thistledb.client;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cz.net21.ttulka.thistledb.server.Server;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author ttulka
 */
public class ClientITest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private Server server;
    private Client client;

    @Before
    public void startServer() throws IOException {
        server = new Server(temp.newFolder().toPath());
        server.start(5000);
    }

    @Before
    public void createClient() {
        client = new Client();
    }

    @After
    public void stopServer() {
        server.stop(5000);
    }

    @Test
    public void basicCommandTest() {
        client.executeCommand("CREATE test");
    }
}
