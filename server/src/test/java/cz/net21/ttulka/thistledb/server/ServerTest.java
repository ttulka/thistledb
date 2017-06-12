package cz.net21.ttulka.thistledb.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

import org.junit.Test;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;

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

    @Test
    public void checkConnectionPoolMaxThreadsTest() throws Exception {
        try (Server server = new Server()) {

            server.setMaxConnectionPoolThreads(2);

            server.startAndWait(500);

            try (Socket socket1 = new Socket("localhost", Server.DEFAULT_PORT);
                 PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true);
                 BufferedReader in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()))) {
                // okay
                out1.println("SELECT * FROM test");
                assertThat("First connection should be accepted.", in1.readLine(), startsWith("ACCEPTED"));
                assertThat("Result shouldn't be null.", in1.readLine(), notNullValue());

                try (Socket socket2 = new Socket("localhost", Server.DEFAULT_PORT);
                     PrintWriter out2 = new PrintWriter(socket2.getOutputStream(), true);
                     BufferedReader in2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()))) {
                    // okay
                    out2.println("SELECT * FROM test");
                    assertThat("Second connection should be accepted.", in2.readLine(), startsWith("ACCEPTED"));
                    assertThat("Result shouldn't be null.", in2.readLine(), notNullValue());

                    try (Socket socket3 = new Socket("localhost", Server.DEFAULT_PORT);
                         PrintWriter out3 = new PrintWriter(socket3.getOutputStream(), true);
                         BufferedReader in3 = new BufferedReader(new InputStreamReader(socket3.getInputStream()))) {
                        // refused
                        out3.println("SELECT * FROM test");
                        assertThat("Third connection should be refused.", in3.readLine(), startsWith("REFUSED"));
                        assertThat("Result should be null.", in3.readLine(), nullValue());

                    } catch (SocketException e) {
                        // ignore
                    }
                }
            }
        }
    }

    @Test
    public void maxClientTimeoutTest() throws Exception {
        try (Server server = new Server()) {

            server.startAndWait(500);

            // default timeout

            try (Socket socket = new Socket("localhost", Server.DEFAULT_PORT);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                Thread.sleep(500);

                out.println("SELECT * FROM test");
                assertThat("Connection should be accepted.", in.readLine(), startsWith("ACCEPTED"));
                assertThat("Result shouldn't be null.", in.readLine(), notNullValue());
            }

            // small timeout

            server.setMaxClientTimeout(10);

            try (Socket socket = new Socket("localhost", Server.DEFAULT_PORT);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                Thread.sleep(500);

                out.println("SELECT * FROM test");
                assertThat("Connection should be accepted.", in.readLine(), startsWith("ACCEPTED"));
                assertThat("Result shouldn't be null.", in.readLine(), notNullValue());

                fail("Client socket cannot exceed the timeout.");

            } catch (SocketException e){
                // ignore
            }

            // normal timeout

            server.setMaxClientTimeout(1000);

            try (Socket socket = new Socket("localhost", Server.DEFAULT_PORT);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                Thread.sleep(500);

                out.println("SELECT * FROM test");
                assertThat("Connection should be accepted.", in.readLine(), startsWith("ACCEPTED"));
                assertThat("Result shouldn't be null.", in.readLine(), notNullValue());
            }
        }
    }
}
