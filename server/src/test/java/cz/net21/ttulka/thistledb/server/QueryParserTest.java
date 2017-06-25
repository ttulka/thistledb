package cz.net21.ttulka.thistledb.server;

import org.json.JSONObject;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class QueryParserTest {

    private final JSONObject json = new JSONObject("{ \"person\": { \"name\": \"John\", surname: \"Smith\", \"age\": 42 } }");

    @Test
    public void validateInsertWrongTest() {
        assertThat(new QueryParser("").validate(), is(false));
        assertThat(new QueryParser("xxx").validate(), is(false));
    }

    @Test
    public void validateSelectTest() {
        assertThat(new QueryParser("SELECT").validate(), is(false));
        assertThat(new QueryParser("SELECT * FROM").validate(), is(false));
        assertThat(new QueryParser("SELECT FROM").validate(), is(false));
        assertThat(new QueryParser("SELECT FROM test").validate(), is(false));
        assertThat(new QueryParser("FROM * SELECT test").validate(), is(false));
        assertThat(new QueryParser("SELECT * FROM test WHERE").validate(), is(false));
        assertThat(new QueryParser("SELECT * FROM test,test2 WHERE 1").validate(), is(false));

        assertThat(new QueryParser("SELECT * FROM test").validate(), is(true));
        assertThat(new QueryParser("SELECT a1 FROM test").validate(), is(true));
        assertThat(new QueryParser("SELECT a1,a_2 FROM test").validate(), is(true));
        assertThat(new QueryParser("SELECT a1, a_2 FROM test").validate(), is(true));
        assertThat(new QueryParser("SELECT a1, a_2 FROM test WHERE 1=1").validate(), is(true));
        assertThat(new QueryParser("SELECT a1, a_2 FROM test WHERE 1=1 AND 1=1").validate(), is(true));
        assertThat(new QueryParser("SELECT a1, a_2 FROM test WHERE 1=1 AND 1=1 OR 1=1").validate(), is(true));
        assertThat(new QueryParser("SELECT a1, a_2 FROM test WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1").validate(), is(true));
    }

    @Test
    public void validateInsertTest() {
        assertThat(new QueryParser("INSERT").validate(), is(false));
        assertThat(new QueryParser("INSERT INTO").validate(), is(false));
        assertThat(new QueryParser("INSERT INTO test").validate(), is(false));
        assertThat(new QueryParser("INSERT INTO test VALUES").validate(), is(false));
        assertThat(new QueryParser("INSERT INTO test VALUES *").validate(), is(false));
        assertThat(new QueryParser("INSERT INTO test,test2 VALUES {}").validate(), is(false));

        assertThat(new QueryParser("INSERT INTO test VALUES {\"a1\":\"1\"}").validate(), is(true));
        assertThat(new QueryParser("INSERT INTO test VALUES {\"a1\" : { \"a_2\":\"1\" }}").validate(), is(true));
        assertThat(new QueryParser("INSERT INTO test VALUES {\"a1\" : { \"a_2\": [\"1\", \"a_2\" ] }}").validate(), is(true));
        assertThat(new QueryParser("INSERT INTO test VALUES {\"a1\" : { \"a_2\": [{\"1\" : \"a_2\"} , {\"1_a\" : \"_a_2_.1\"} ] }}").validate(), is(true));
    }

    @Test
    public void validateUpdateTest() {
        assertThat(new QueryParser("UPDATE").validate(), is(false));
        assertThat(new QueryParser("UPDATE *").validate(), is(false));
        assertThat(new QueryParser("UPDATE test").validate(), is(false));
        assertThat(new QueryParser("UPDATE test SET").validate(), is(false));
        assertThat(new QueryParser("UPDATE test SET WHERE").validate(), is(false));
        assertThat(new QueryParser("UPDATE test SET x.a_1 WHERE").validate(), is(false));
        assertThat(new QueryParser("UPDATE test SET x.a_1 WHERE 1=1").validate(), is(false));
        assertThat(new QueryParser("UPDATE test SET x.a_1 = WHERE 1=1").validate(), is(false));
        assertThat(new QueryParser("UPDATE * SET x.a_1 = 1 WHERE 1=1").validate(), is(false));

        assertThat(new QueryParser("UPDATE test SET x.a_1 = 1").validate(), is(true));
        assertThat(new QueryParser("UPDATE test SET x.a_1 = '1'").validate(), is(true));
        assertThat(new QueryParser("UPDATE test SET x.a_1 = y.a_1").validate(), is(true));
        assertThat(new QueryParser("UPDATE test SET x.a_1 = 'y.a_1'").validate(), is(true));
        assertThat(new QueryParser("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1").validate(), is(true));
        assertThat(new QueryParser("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND 1=1").validate(), is(true));
        assertThat(new QueryParser("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND 1=1 OR 1=1").validate(), is(true));
        assertThat(new QueryParser("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1").validate(), is(true));
    }

    @Test
    public void validateDeleteTest() {
        assertThat(new QueryParser("DELETE").validate(), is(false));
        assertThat(new QueryParser("DELETE FROM").validate(), is(false));
        assertThat(new QueryParser("DELETE FROM *").validate(), is(false));
        assertThat(new QueryParser("DELETE test FROM test").validate(), is(false));
        assertThat(new QueryParser("DELETE FROM test,test1").validate(), is(false));

        assertThat(new QueryParser("DELETE FROM test").validate(), is(true));
        assertThat(new QueryParser("DELETE FROM test WHERE 1=1").validate(), is(true));
        assertThat(new QueryParser("DELETE FROM test WHERE 1=1 AND 1=1").validate(), is(true));
        assertThat(new QueryParser("DELETE FROM test WHERE 1=1 AND 1=1 OR 1=1").validate(), is(true));
        assertThat(new QueryParser("DELETE FROM test WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1").validate(), is(true));
    }

    @Test
    public void validateCreateIndexTest() {
        assertThat(new QueryParser("CREATE INDEX").validate(), is(false));
        assertThat(new QueryParser("CREATE INDEX a_2.a_3").validate(), is(false));
        assertThat(new QueryParser("CREATE INDEX a_2.a_3 ON").validate(), is(false));
        assertThat(new QueryParser("CREATE INDEX a_2.a_3 ON a_2.a_3").validate(), is(false));

        assertThat(new QueryParser("CREATE INDEX a_2.a_3 ON a_2").validate(), is(true));
    }

    @Test
    public void validateDropIndexTest() {
        assertThat(new QueryParser("DROP INDEX").validate(), is(false));
        assertThat(new QueryParser("DROP INDEX a_2.a_3").validate(), is(false));
        assertThat(new QueryParser("DROP INDEX a_2.a_3 ON").validate(), is(false));
        assertThat(new QueryParser("DROP INDEX a_2.a_3 ON a_2.a_3").validate(), is(false));

        assertThat(new QueryParser("DROP INDEX a_2.a_3 ON a_2").validate(), is(true));
    }

    @Test
    public void validateCreateTest() {
        assertThat(new QueryParser("CREATE").validate(), is(false));
        assertThat(new QueryParser("CREATE *").validate(), is(false));
        assertThat(new QueryParser("CREATE a_2.a_3").validate(), is(false));

        assertThat(new QueryParser("CREATE a_2").validate(), is(false));
    }

    @Test
    public void validateDropTest() {
        assertThat(new QueryParser("DROP").validate(), is(false));
        assertThat(new QueryParser("DROP *").validate(), is(false));
        assertThat(new QueryParser("DROP a_2.a_3").validate(), is(false));

        assertThat(new QueryParser("DROP a_2").validate(), is(false));
    }

    @Test
    public void parseCommandTest() {
        Commands cmd = QueryParser.parseCommand("SELECT FROM test");
        assertThat(cmd, is(Commands.SELECT));

        cmd = QueryParser.parseCommand("select FROM test");
        assertThat(cmd, is(Commands.SELECT));

        cmd = QueryParser.parseCommand("sEleCt FROM test");
        assertThat(cmd, is(Commands.SELECT));

        cmd = QueryParser.parseCommand("SELECT");
        assertThat(cmd, is(Commands.SELECT));
    }

    @Test
    public void parseCollectionForSelectTest() {
        String result = new QueryParser("SELECT * FROM test").parseCollection();
        assertThat(result, is("test"));

        result = new QueryParser("SELECT col1, col2 FROM test WHERE person.name = 'John'").parseCollection();
        assertThat(result, is("test"));
    }

    @Test
    public void parseCollectionForInsertTest() {
        String result = new QueryParser("INSERT INTO test VALUES " + json).parseCollection();
        assertThat(result, is("test"));
    }

    @Test
    public void parseColumnsTest() {
        String result = new QueryParser("SELECT * FROM test").parseColumns();
        assertThat(result, is("*"));

        result = new QueryParser("SELECT col1, col2 FROM test").parseColumns();
        assertThat(result, is("col1,col2"));
    }

    @Test
    public void parseWhereTest() {
        String result = new QueryParser("SELECT * FROM test WHERE 1=1").parseWhere();
        assertThat(result, is("1=1"));

        result = new QueryParser("SELECT col1, col2 FROM test wHeRe 1=1 AND 2=2").parseWhere();
        assertThat(result, is("1=1 AND 2=2"));
    }

    @Test
    public void parseValues() {
        String result = new QueryParser("INSERT INTO test VALUES " + json).parseValues();
        assertThat(result, is(json.toString()));
    }
}
