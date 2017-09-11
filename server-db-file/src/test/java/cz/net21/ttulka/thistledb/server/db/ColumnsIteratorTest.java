package cz.net21.ttulka.thistledb.server.db;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import cz.net21.ttulka.thistledb.server.TestData;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * @author ttulka
 */
public class ColumnsIteratorTest {

    @Test
    public void basicTest() {
        ColumnsIterator columnsIterator = new ColumnsIterator(TestData.TSON_PERSON);

        List<String> columns = new ArrayList<>();
        while (columnsIterator.hasNext()) {
            columns.add(columnsIterator.next());
        }

        assertThat(columns, containsInAnyOrder("person.name", "person.surname", "person.age"));
    }
}
