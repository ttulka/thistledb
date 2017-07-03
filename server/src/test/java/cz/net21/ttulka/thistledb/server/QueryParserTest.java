package cz.net21.ttulka.thistledb.server;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class QueryParserTest {

    @Test(expected = IllegalArgumentException.class)
    public void validateInsertWrongTest() {
        new QueryParser("xxx");
    }

    @Test
    public void validateSelectTest() {
        assertThat(QueryParser.SELECT.matcher("SELECT").matches(), is(false));
        assertThat(QueryParser.SELECT.matcher("SELECT * FROM").matches(), is(false));
        assertThat(QueryParser.SELECT.matcher("SELECT FROM").matches(), is(false));
        assertThat(QueryParser.SELECT.matcher("SELECT FROM test").matches(), is(false));
        assertThat(QueryParser.SELECT.matcher("FROM * SELECT test").matches(), is(false));
        assertThat(QueryParser.SELECT.matcher("SELECT * FROM test WHERE").matches(), is(false));
        assertThat(QueryParser.SELECT.matcher("SELECT * FROM test,test2 WHERE 1").matches(), is(false));

        assertThat(QueryParser.SELECT.matcher("SELECT * FROM test").matches(), is(true));
        assertThat(QueryParser.SELECT.matcher("SELECT a1 FROM test").matches(), is(true));
        assertThat(QueryParser.SELECT.matcher("SELECT a1,a_2 FROM test").matches(), is(true));
        assertThat(QueryParser.SELECT.matcher("SELECT a1, a_2 FROM test").matches(), is(true));
        assertThat(QueryParser.SELECT.matcher("SELECT a1, a_2 FROM test WHERE 1=1").matches(), is(true));
        assertThat(QueryParser.SELECT.matcher("SELECT a1, a_2 FROM test WHERE 1=1 AND 1=1").matches(), is(true));
        assertThat(QueryParser.SELECT.matcher("SELECT a1, a_2 FROM test WHERE 1=1 AND 1=1 OR 1=1").matches(), is(true));
        assertThat(QueryParser.SELECT.matcher("SELECT a1, a_2 FROM test WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1").matches(), is(true));
    }

    @Test
    public void validateInsertTest() {
        assertThat(QueryParser.INSERT.matcher("INSERT").matches(), is(false));
        assertThat(QueryParser.INSERT.matcher("INSERT INTO").matches(), is(false));
        assertThat(QueryParser.INSERT.matcher("INSERT INTO test").matches(), is(false));
        assertThat(QueryParser.INSERT.matcher("INSERT INTO test VALUES").matches(), is(false));
        assertThat(QueryParser.INSERT.matcher("INSERT INTO test VALUES *").matches(), is(false));
        assertThat(QueryParser.INSERT.matcher("INSERT INTO test,test2 VALUES {}").matches(), is(false));

        assertThat(QueryParser.INSERT.matcher("INSERT INTO test VALUES {}").matches(), is(true));
        assertThat(QueryParser.INSERT.matcher("INSERT INTO test VALUES {\"a1\":\"1\"}").matches(), is(true));
        assertThat(QueryParser.INSERT.matcher("INSERT INTO test VALUES {\"a1\" : { \"a_2\":\"1\" }}").matches(), is(true));
        assertThat(QueryParser.INSERT.matcher("INSERT INTO test VALUES {\"a1\" : { \"a_2\": [\"1\", \"a_2\" ] }}").matches(), is(true));
        assertThat(QueryParser.INSERT.matcher("INSERT INTO test VALUES {\"a1\" : { \"a_2\": [{\"1\" : \"a_2\"} , {\"1_a\" : \"_a_2_.1\"} ] }}").matches(), is(true));
    }

    @Test
    public void validateUpdateTest() {
        assertThat(QueryParser.UPDATE.matcher("UPDATE").matches(), is(false));
        assertThat(QueryParser.UPDATE.matcher("UPDATE *").matches(), is(false));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test").matches(), is(false));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET").matches(), is(false));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET WHERE").matches(), is(false));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 WHERE").matches(), is(false));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 WHERE 1=1").matches(), is(false));
        assertThat(QueryParser.UPDATE.matcher("UPDATE * SET x.a_1 = 1 WHERE 1=1").matches(), is(false));

        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 = 1").matches(), is(true));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 = '1'").matches(), is(true));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 = y.a_1").matches(), is(true));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 = 'y.a_1'").matches(), is(true));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1").matches(), is(true));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND 1=1").matches(), is(true));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND 1=1 OR 1=1").matches(), is(true));
        assertThat(QueryParser.UPDATE.matcher("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1").matches(), is(true));
    }

    @Test
    public void validateDeleteTest() {
        assertThat(QueryParser.DELETE.matcher("DELETE").matches(), is(false));
        assertThat(QueryParser.DELETE.matcher("DELETE FROM").matches(), is(false));
        assertThat(QueryParser.DELETE.matcher("DELETE FROM *").matches(), is(false));
        assertThat(QueryParser.DELETE.matcher("DELETE test FROM test").matches(), is(false));
        assertThat(QueryParser.DELETE.matcher("DELETE FROM test,test1").matches(), is(false));

        assertThat(QueryParser.DELETE.matcher("DELETE FROM test").matches(), is(true));
        assertThat(QueryParser.DELETE.matcher("DELETE FROM test WHERE 1=1").matches(), is(true));
        assertThat(QueryParser.DELETE.matcher("DELETE FROM test WHERE 1=1 AND 1=1").matches(), is(true));
        assertThat(QueryParser.DELETE.matcher("DELETE FROM test WHERE 1=1 AND 1=1 OR 1=1").matches(), is(true));
        assertThat(QueryParser.DELETE.matcher("DELETE FROM test WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1").matches(), is(true));
    }

    @Test
    public void validateCreateIndexTest() {
        assertThat(QueryParser.CREATE_INDEX.matcher("CREATE INDEX").matches(), is(false));
        assertThat(QueryParser.CREATE_INDEX.matcher("CREATE INDEX a_2.a_3").matches(), is(false));
        assertThat(QueryParser.CREATE_INDEX.matcher("CREATE INDEX a_2.a_3 ON").matches(), is(false));
        assertThat(QueryParser.CREATE_INDEX.matcher("CREATE INDEX a_2.a_3 ON a_2.a_3").matches(), is(false));

        assertThat(QueryParser.CREATE_INDEX.matcher("CREATE INDEX a_2.a_3 ON a_2").matches(), is(true));
    }

    @Test
    public void validateDropIndexTest() {
        assertThat(QueryParser.DROP_INDEX.matcher("DROP INDEX").matches(), is(false));
        assertThat(QueryParser.DROP_INDEX.matcher("DROP INDEX a_2.a_3").matches(), is(false));
        assertThat(QueryParser.DROP_INDEX.matcher("DROP INDEX a_2.a_3 ON").matches(), is(false));
        assertThat(QueryParser.DROP_INDEX.matcher("DROP INDEX a_2.a_3 ON a_2.a_3").matches(), is(false));

        assertThat(QueryParser.DROP_INDEX.matcher("DROP INDEX a_2.a_3 ON a_2").matches(), is(true));
    }

    @Test
    public void validateCreateTest() {
        assertThat(QueryParser.CREATE.matcher("CREATE").matches(), is(false));
        assertThat(QueryParser.CREATE.matcher("CREATE *").matches(), is(false));
        assertThat(QueryParser.CREATE.matcher("CREATE a_2.a_3").matches(), is(false));

        assertThat(QueryParser.CREATE.matcher("CREATE a_2").matches(), is(true));
    }

    @Test
    public void validateDropTest() {
        assertThat(QueryParser.DROP.matcher("DROP").matches(), is(false));
        assertThat(QueryParser.DROP.matcher("DROP *").matches(), is(false));
        assertThat(QueryParser.DROP.matcher("DROP a_2.a_3").matches(), is(false));

        assertThat(QueryParser.DROP.matcher("DROP a_2").matches(), is(true));
    }

    @Test
    public void parseCommandTest() {
        Commands cmd = QueryParser.parseCommand("SELECT * FROM test");
        assertThat(cmd, is(Commands.SELECT));

        cmd = QueryParser.parseCommand("INSERT INTO test VALUES {}");
        assertThat(cmd, is(Commands.INSERT));

        cmd = QueryParser.parseCommand("UPDATE test SET 1=1");
        assertThat(cmd, is(Commands.UPDATE));

        cmd = QueryParser.parseCommand("DELETE FROM test");
        assertThat(cmd, is(Commands.DELETE));

        cmd = QueryParser.parseCommand("CREATE test");
        assertThat(cmd, is(Commands.CREATE));

        cmd = QueryParser.parseCommand("DROP test");
        assertThat(cmd, is(Commands.DROP));

        cmd = QueryParser.parseCommand("CREATE INDEX test ON test");
        assertThat(cmd, is(Commands.CREATE_INDEX));

        cmd = QueryParser.parseCommand("DROP INDEX test ON test");
        assertThat(cmd, is(Commands.DROP_INDEX));
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
        String result = new QueryParser("INSERT INTO test VALUES " + TestData.JSON_PERSON).parseCollection();
        assertThat(result, is("test"));
    }

    @Test
    public void parseCollectionForUpdateTest() {
        String result = new QueryParser("UPDATE test SET 1=1").parseCollection();
        assertThat(result, is("test"));
    }

    @Test
    public void parseCollectionForDeleteTest() {
        String result = new QueryParser("DELETE FROM test WHERE 1=1").parseCollection();
        assertThat(result, is("test"));
    }

    @Test
    public void parseCollectionForCreateTest() {
        String result = new QueryParser("CREATE test").parseCollection();
        assertThat(result, is("test"));
    }

    @Test
    public void parseCollectionForDropTest() {
        String result = new QueryParser("DROP test").parseCollection();
        assertThat(result, is("test"));
    }

    @Test
    public void parseCollectionForCreateIndexTest() {
        String result = new QueryParser("CREATE INDEX my.test1 ON test").parseCollection();
        assertThat(result, is("test"));
    }

    @Test
    public void parseCollectionForDropIndexTest() {
        String result = new QueryParser("DROP INDEX my.test1 ON test").parseCollection();
        assertThat(result, is("test"));
    }

    @Test
    public void parseColumnsTest() {
        String result = new QueryParser("SELECT * FROM test").parseColumns();
        assertThat(result, is("*"));

        result = new QueryParser("SELECT col1 FROM test").parseColumns();
        assertThat(result, is("col1"));

        result = new QueryParser("SELECT col1, col2 FROM test").parseColumns();
        assertThat(result, is("col1,col2"));

        result = new QueryParser("CREATE INDEX col1 ON test").parseColumns();
        assertThat(result, is("col1"));

        result = new QueryParser("DROP INDEX col1 ON test").parseColumns();
        assertThat(result, is("col1"));
    }

    @Test
    public void parseWhereTest() {
        String result = new QueryParser("SELECT * FROM test WHERE 1=1").parseWhere();
        assertThat(result, is("1=1"));

        result = new QueryParser("SELECT col1, col2 FROM test wHeRe 1=1 AND 2=2").parseWhere();
        assertThat(result, is("1=1 AND 2=2"));

        result = new QueryParser("UPDATE test SET a.b='1', b.c=2 WHERE 1=1 AND 2=2").parseWhere();
        assertThat(result, is("1=1 AND 2=2"));

        result = new QueryParser("DELETE FROM test WHERE 1=1 AND 2=2").parseWhere();
        assertThat(result, is("1=1 AND 2=2"));
    }

    @Test
    public void parseValuesTest() {
        String result = new QueryParser("INSERT INTO test VALUES " + TestData.JSON_PERSON).parseValues();
        assertThat(result, is(TestData.JSON_PERSON));
    }

    @Test
    public void parseSetColumnsTest() {
        String[] result = new QueryParser("UPDATE test SET a.b='1', b.c=2 WHERE 1=1 AND 2=2").parseSetColumns();
        assertThat(String.join(",", result), is("a.b,b.c"));
    }

    @Test
    public void parseSetValuesTest() {
        String[] result = new QueryParser("UPDATE test SET a.b='1', b.c=2 WHERE 1=1 AND 2=2").parseSetValues();
        assertThat(String.join(",", result), is("'1',2"));
    }
}
