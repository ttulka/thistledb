package cz.net21.ttulka.thistledb.server;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Created by ttulka
 */
public class ServerTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void startServerTest() throws Exception {
        Server server = new Server(temp.newFolder().toPath());

        assertThat("Server doesn't listen before been started.", server.listening(), is(false));

        server.startAndWait(500);

        assertThat("Server listens after been started.", server.listening(), is(true));

        server.stop();

        assertThat("Server doesn't listen after been stopped.", server.listening(), is(false));
    }

    @Test
    public void readDataTest() throws Exception {
        QueryProcessor queryProcessorMock = mock(QueryProcessor.class);

        try (Server server = new Server(temp.newFolder().toPath()) {
            @Override
            protected QueryProcessor createQueryProcessor() {
                return queryProcessorMock;
            }
        }) {
            server.startAndWait(500);

            try (Socket socket = new Socket("localhost", Server.DEFAULT_PORT);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                socket.setSoTimeout(1000);
                out.println("SELECT 1 FROM dual");
                out.println("SELECT 2 FROM dual");
                out.println("SELECT 3 FROM dual");
            }

            Thread.sleep(1000);

            verify(queryProcessorMock).process(eq("SELECT 1 FROM dual"), any());
            verify(queryProcessorMock).process(eq("SELECT 2 FROM dual"), any());
            verify(queryProcessorMock).process(eq("SELECT 3 FROM dual"), any());

            verifyNoMoreInteractions(queryProcessorMock);
        }
    }

    @Test
    public void moreQueriesTest() throws Exception {
        try (Server server = new Server(temp.newFolder().toPath())) {
            server.startAndWait(500);

            try (Socket socket = new Socket("localhost", Server.DEFAULT_PORT);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                socket.setSoTimeout(1000);

                out.println("SELECT name FROM dual");
                assertThat("First connection should be accepted.", in.readLine(), is("ACCEPTED"));
                assertThat("First result shouldn't be null.", in.readLine(), is("{\"name\":\"DUAL\"}"));
                assertThat("First connection should be finished.", in.readLine(), is("FINISHED"));

                out.println("SELECT name FROM dual");
                assertThat("Second connection should be accepted.", in.readLine(), startsWith("ACCEPTED"));
                assertThat("Second result shouldn't be null.", in.readLine(), is("{\"name\":\"DUAL\"}"));
                assertThat("Second connection should be finished.", in.readLine(), is("FINISHED"));
            }
        }
    }

    @Test(expected = SocketException.class)
    public void checkConnectionPoolMaxThreadsTest() throws Exception {
        try (Server server = new Server(temp.newFolder().toPath())) {
            server.startAndWait(500);

            server.setMaxClientConnections(2);

            try (Socket socket1 = new Socket("localhost", Server.DEFAULT_PORT);
                 PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true);
                 BufferedReader in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()))) {
                socket1.setSoTimeout(1000);

                try (Socket socket2 = new Socket("localhost", Server.DEFAULT_PORT);
                     PrintWriter out2 = new PrintWriter(socket2.getOutputStream(), true);
                     BufferedReader in2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()))) {
                    socket2.setSoTimeout(1000);
                    try (Socket socket3 = new Socket("localhost", Server.DEFAULT_PORT);
                         PrintWriter out3 = new PrintWriter(socket3.getOutputStream(), true);
                         BufferedReader in3 = new BufferedReader(new InputStreamReader(socket3.getInputStream()))) {
                        socket3.setSoTimeout(1000);

                        // okay
                        out1.println("SELECT 1 FROM dual");
                        assertThat("First connection should be accepted.", in1.readLine(), is("ACCEPTED"));
                        assertThat("First result shouldn't be null.", in1.readLine(), is("{\"value\":1}"));
                        assertThat("First connection should be finished.", in1.readLine(), is("FINISHED"));

                        // okay
                        out2.println("SELECT 2 FROM dual");
                        assertThat("Second connection should be accepted.", in2.readLine(), is("ACCEPTED"));
                        assertThat("Second result shouldn't be null.", in2.readLine(), is("{\"value\":2}"));
                        assertThat("Second connection should be finished.", in2.readLine(), is("FINISHED"));

                        // refused
                        out3.println("SELECT 3 FROM dual");
                        assertThat("Third connection should be refused.", in3.readLine(), startsWith("REFUSED"));
                        assertThat("Third result should be null.", in3.readLine(), nullValue());

                        out3.println("SELECT 4 FROM dual");
                        fail("After the connection was refused any attempt to query the server will fail.");
                    }
                }
            }
        }
    }
}
