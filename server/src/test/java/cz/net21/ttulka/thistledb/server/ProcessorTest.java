package cz.net21.ttulka.thistledb.server;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import cz.net21.ttulka.thistledb.server.db.DataSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;

/**
 * Created by ttulka
 */
@RunWith(MockitoJUnitRunner.class)
public class ProcessorTest {

    @Mock
    private DataSource dataSource;

    private final Processor processor = new Processor(dataSource);

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
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        processor.acceptCommand(Commands.SELECT, writer);
        assertThat(out.get(0), is(Processor.ACCEPTED + " " + Commands.SELECT));
    }

    @Test
    public void selectTest() {
        List<String> out = new ArrayList<>();
        PrintWriter writer = mockPrintWriter(out);

        processor.process("SELECT * FROM test", writer);
        assertThat(out.get(0), is(Processor.ACCEPTED + " " + Commands.SELECT));
        // TODO
    }

    private PrintWriter mockPrintWriter(List<String> out) {
        PrintWriter writer = mock(PrintWriter.class);
        Mockito.doAnswer((s) -> out.add(s.getArgumentAt(0, String.class))).when(writer).println(anyString());
        return writer;
    }
}
