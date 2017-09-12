package cz.net21.ttulka.thistledb.server;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import cz.net21.ttulka.thistledb.db.DataSource;
import reactor.core.publisher.Flux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author ttulka
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryProcessorTest {

    @Mock
    private DataSource dataSource;

    @InjectMocks
    private QueryProcessor queryProcessor;

    @Before
    public void setUp() {
        when(dataSource.select(eq("test"), anyString(), any())).thenReturn(Flux.just(TestData.JSON_PERSON));
        when(dataSource.select(eq("test_multiple"), anyString(), any())).thenReturn(Flux.just(TestData.JSON_PERSON, TestData.JSON_PERSON));
    }

    @Test
    public void acceptQueryTest() {
        String result = queryProcessor.acceptQuery(Commands.SELECT);
        assertThat(result, is(QueryProcessor.ACCEPTED + " " + Commands.SELECT));
    }

    @Test
    public void selectTest() {
        List<String> out = new ArrayList<>();
        Consumer<String> writer = mockWriter(out);

        queryProcessor.process("SELECT * FROM test", writer);

        verify(dataSource).select(anyString(), anyString(), any());

        assertThat(out.size(), is(3));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(TestData.JSON_PERSON));
        assertThat(out.get(2), is(QueryProcessor.FINISHED));
    }

    @Test
    public void selectMultipleResultTest() {
        List<String> out = new ArrayList<>();
        Consumer<String> writer = mockWriter(out);

        queryProcessor.process("SELECT * FROM test_multiple", writer);

        verify(dataSource).select(anyString(), anyString(), any());

        assertThat(out.size(), is(4));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(TestData.JSON_PERSON));
        assertThat(out.get(2), is(TestData.JSON_PERSON));
        assertThat(out.get(3), is(QueryProcessor.FINISHED));
    }

    @Test
    public void updateTest() {
        List<String> out = new ArrayList<>();
        Consumer<String> writer = mockWriter(out);

        queryProcessor.process("UPDATE test SET a=1", writer);

        verify(dataSource).update(anyString(), any(), any(), any());

        assertThat(out.size(), is(3));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
        assertThat(out.get(2), is(QueryProcessor.FINISHED));
    }

    @Test
    public void deleteTest() {
        List<String> out = new ArrayList<>();
        Consumer<String> writer = mockWriter(out);

        queryProcessor.process("DELETE FROM test", writer);

        verify(dataSource).delete(anyString(), anyString());

        assertThat(out.size(), is(3));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
        assertThat(out.get(2), is(QueryProcessor.FINISHED));
    }

    @Test
    public void createTest() {
        List<String> out = new ArrayList<>();
        Consumer<String> writer = mockWriter(out);

        queryProcessor.process("CREATE test", writer);

        verify(dataSource).createCollection(anyString());

        assertThat(out.size(), is(3));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
        assertThat(out.get(2), is(QueryProcessor.FINISHED));
    }

    @Test
    public void dropTest() {
        List<String> out = new ArrayList<>();
        Consumer<String> writer = mockWriter(out);

        queryProcessor.process("DROP test", writer);

        verify(dataSource).dropCollection(anyString());

        assertThat(out.size(), is(3));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
        assertThat(out.get(2), is(QueryProcessor.FINISHED));
    }

    @Test
    public void createIndexTest() {
        List<String> out = new ArrayList<>();
        Consumer<String> writer = mockWriter(out);

        queryProcessor.process("CREATE INDEX a ON test", writer);

        verify(dataSource).createIndex(anyString(), anyString());

        assertThat(out.size(), is(3));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
        assertThat(out.get(2), is(QueryProcessor.FINISHED));
    }

    @Test
    public void dropIndexTest() {
        List<String> out = new ArrayList<>();
        Consumer<String> writer = mockWriter(out);

        queryProcessor.process("DROP INDEX a ON test", writer);

        verify(dataSource).dropIndex(anyString(), anyString());

        assertThat(out.size(), is(3));
        assertThat(out.get(0), is(QueryProcessor.ACCEPTED));
        assertThat(out.get(1), is(QueryProcessor.OKAY));
        assertThat(out.get(2), is(QueryProcessor.FINISHED));
    }

    private Consumer<String> mockWriter(List<String> out) {
        Consumer<String> writer = mock(Consumer.class);
        Mockito.doAnswer((s) -> out.add(s.getArgumentAt(0, String.class))).when(writer).accept(anyString());
        return writer;
    }
}
