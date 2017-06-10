package cz.net21.ttulka.thistledb.server;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.json.JSONObject;
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
public class ProcessorTest {

    private final JSONObject json = new JSONObject("{ \"person\": { \"name\": \"John\", surname: \"Smith\", \"age\": 42 } }");

    @Mock
    private DataSource dataSource;

    @InjectMocks
    private Processor processor;

    @Before
    public void setUp() {
        when(dataSource.select(eq("test"), anyString(), any())).thenReturn(Flux.just(json));
        when(dataSource.select(eq("test_multiple"), anyString(), any())).thenReturn(Flux.just(json, json));
    }

    @Test
    public void parseCommandTest() {
        Commands cmd = processor.parseCommand("SELECT FROM test");
        assertThat(cmd, is(Commands.SELECT));

        cmd = processor.parseCommand("select FROM test");
        assertThat(cmd, is(Commands.SELECT));

        cmd = processor.parseCommand("sEleCt FROM test");
        assertThat(cmd, is(Commands.SELECT));

        cmd = processor.parseCommand("SELECT");
        assertThat(cmd, is(Commands.SELECT));
    }

    @Test
    public void acceptCommandTest() {
        String result = processor.acceptCommand(Commands.SELECT);
        assertThat(result, is(Processor.ACCEPTED + " " + Commands.SELECT));
    }

    @Test
    public void parseCollectionForSelectTest() {
        String result = processor.parseCollection("SELECT * FROM test");
        assertThat(result, is("test"));

        result = processor.parseCollection("SELECT col1, col2 FROM test WHERE person.name = 'John'");
        assertThat(result, is("test"));
    }

    @Test
    public void parseCollectionForInsertTest() {
        String result = processor.parseCollection("INSERT INTO test VALUES " + json);
        assertThat(result, is("test"));
    }

    @Test
    public void parseColumnsTest() {
        String result = processor.parseColumns("SELECT * FROM test");
        assertThat(result, is("*"));

        result = processor.parseColumns("SELECT col1, col2 FROM test");
        assertThat(result, is("col1,col2"));
    }

    @Test
    public void parseWhereTest() {
        String result = processor.parseWhere("SELECT * FROM test WHERE 1=1");
        assertThat(result, is("1=1"));

        result = processor.parseWhere("SELECT col1, col2 FROM test wHeRe 1=1 AND 2=2");
        assertThat(result, is("1=1 AND 2=2"));
    }

    @Test
    public void parseValues() {
        String result = processor.parseValues("INSERT INTO test VALUES " + json);
        assertThat(result, is(json.toString()));
    }

    @Test
    public void selectSingleResultTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        processor.process("SELECT * FROM test", writer);

        assertThat(out.size(), is(2));
        assertThat(out.get(0), is(Processor.ACCEPTED));
        assertThat(out.get(1), is(json.toString()));
    }

    @Test
    public void selectMultipleResultTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        processor.process("SELECT * FROM test_multiple", writer);

        assertThat(out.size(), is(3));
        assertThat(out.get(0), is(Processor.ACCEPTED));
        assertThat(out.get(1), is(json.toString()));
        assertThat(out.get(2), is(json.toString()));
    }

    private PrintWriter mockPrintWriter(List<String> out) {
        PrintWriter writer = mock(PrintWriter.class);
        Mockito.doAnswer((s) -> out.add(s.getArgumentAt(0, String.class))).when(writer).println(anyString());
        return writer;
    }
}
