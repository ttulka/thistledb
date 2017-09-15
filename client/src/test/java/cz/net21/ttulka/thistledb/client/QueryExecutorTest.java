package cz.net21.ttulka.thistledb.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author ttulka
 */
public class QueryExecutorTest {

    private static final String QUERY = "SELECT * FROM test";
    private static final String RESULT = QueryExecutor.OKAY;

    private Socket socket;

    public void setUp(List<String> in, List<String> out) {
        socket = mock(Socket.class);
        try {
            when(socket.getInputStream()).thenReturn(new InputStream() {
                private int index, position;

                @Override
                public int read() throws IOException {
                    if (in.size() <= index) {
                        return -1;
                    }
                    if (in.get(index).length() <= position) {
                        index++;
                        position = 0;
                        return '\n';
                    } else {
                        return in.get(index).charAt(position++);
                    }
                }
            });
            when(socket.getOutputStream()).thenReturn(new OutputStream() {
                StringBuilder sb = new StringBuilder();

                @Override
                public void write(int b) throws IOException {
                    if (b == '\n') {
                        out.add(sb.toString());
                        sb = new StringBuilder();
                    } else {
                        sb.append((char) b);
                    }
                }
            });
        } catch (IOException e) {
            // ignore
        }
    }

    @Test
    public void executeQueryTest() {
        List<String> in = Arrays.asList(RESULT);
        List<String> out = new ArrayList<>();
        setUp(in, out);

        QueryExecutor queryExecutor = new QueryExecutor(socket, QUERY);
        queryExecutor.executeQuery();

        assertThat(out.get(0), startsWith(QUERY));
    }

    @Test
    public void getNextResultTest() {
        List<String> in = Arrays.asList(RESULT);
        List<String> out = new ArrayList<>();
        setUp(in, out);

        QueryExecutor queryExecutor = new QueryExecutor(socket, QUERY);
        queryExecutor.executeQuery();

        String result = queryExecutor.getNextResult();

        assertThat(out.get(0), startsWith(QUERY));
        assertThat(result, containsString("\"status\":\"" + RESULT.toLowerCase() + "\""));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void getNextResultExceptionTest() {
        List<String> in = Arrays.asList(RESULT);
        List<String> out = new ArrayList<>();
        setUp(in, out);

        QueryExecutor queryExecutor = new QueryExecutor(socket, QUERY);
        queryExecutor.getNextResult();

        fail("getNextResult() cannot be called before executeQuery().");
    }
}
