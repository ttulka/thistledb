package cz.net21.ttulka.thistledb.server;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import reactor.core.publisher.Flux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ttulka
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryProcessorTest {

    @Mock
    private DataSource dataSource;

    @InjectMocks
    private QueryProcessor queryProcessor;

    @Before
    public void setUp() {
        when(dataSource.select(eq("test"), anyString(), any())).thenReturn(Flux.just(TestData.TSON_PERSON));
        when(dataSource.select(eq("test_multiple"), anyString(), any())).thenReturn(Flux.just(TestData.TSON_PERSON, TestData.TSON_PERSON));
    }

    @Test
    public void acceptQueryTest() {
        String result = queryProcessor.acceptQuery(Commands.SELECT);
        assertThat(result, is(QueryProcessor.ACCEPTED + " " + Commands.SELECT));
    }

    @Test
    public void selectTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        queryProcessor.process("SELECT * FROM test", writer);

        assertThat(out.size(), is(2));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(TestData.JSON_PERSON));
    }

    @Test
    public void selectMultipleResultTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        queryProcessor.process("SELECT * FROM test_multiple", writer);

        assertThat(out.size(), is(3));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(TestData.JSON_PERSON));
        assertThat(out.get(2), is(TestData.JSON_PERSON));
    }

    @Test
    public void updateTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        queryProcessor.process("UPDATE test SET a=1", writer);

        assertThat(out.size(), is(2));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
    }

    @Test
    public void deleteTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        queryProcessor.process("DELETE FROM test", writer);

        assertThat(out.size(), is(2));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
    }

    @Test
    public void createTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        queryProcessor.process("CREATE test", writer);

        assertThat(out.size(), is(2));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
    }

    @Test
    public void dropTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        queryProcessor.process("DROP test", writer);

        assertThat(out.size(), is(2));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
    }

    @Test
    public void createIndexTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        queryProcessor.process("CREATE INDEX a ON test", writer);

        assertThat(out.size(), is(2));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
    }

    @Test
    public void dropIndexTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        queryProcessor.process("DROP INDEX a ON test", writer);

        assertThat(out.size(), is(2));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
    }

    private PrintWriter mockPrintWriter(List<String> out) {
        PrintWriter writer = mock(PrintWriter.class);
        Mockito.doAnswer((s) -> out.add(s.getArgumentAt(0, String.class))).when(writer).println(anyString());
        return writer;
    }
}
