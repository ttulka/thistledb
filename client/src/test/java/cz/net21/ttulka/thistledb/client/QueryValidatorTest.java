package cz.net21.ttulka.thistledb.client;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class QueryValidatorTest {

    @Test
    public void validateSelectTest() {
        assertThat(QueryValidator.validate("SELECT"), is(false));
        assertThat(QueryValidator.validate("SELECT * FROM"), is(false));
        assertThat(QueryValidator.validate("SELECT FROM"), is(false));
        assertThat(QueryValidator.validate("SELECT FROM test"), is(false));
        assertThat(QueryValidator.validate("FROM * SELECT test"), is(false));
        assertThat(QueryValidator.validate("SELECT * FROM test WHERE"), is(false));
        assertThat(QueryValidator.validate("SELECT * FROM test,test2 WHERE 1"), is(false));
        assertThat(QueryValidator.validate("select * from test where person.name = \"Johnny"), is(false));

        assertThat(QueryValidator.validate("select * from test where person.name = \"Johnny\""), is(true));
        assertThat(QueryValidator.validate("SELECT * FROM test"), is(true));
        assertThat(QueryValidator.validate("SELECT a1 FROM test"), is(true));
        assertThat(QueryValidator.validate("SELECT a1 FROM test WHERE 1=1"), is(true));
        assertThat(QueryValidator.validate("SELECT a_2 FROM test WHERE 1=1 AND 1=1"), is(true));
        assertThat(QueryValidator.validate("SELECT a_2 FROM test WHERE 1=1 AND 1=1 OR 1=1"), is(true));
        assertThat(QueryValidator.validate("SELECT a_2 FROM test WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1"), is(true));
    }

    @Test
    public void validateInsertTest() {
        assertThat(QueryValidator.validate("INSERT"), is(false));
        assertThat(QueryValidator.validate("INSERT INTO"), is(false));
        assertThat(QueryValidator.validate("INSERT INTO test"), is(false));
        assertThat(QueryValidator.validate("INSERT INTO test VALUES"), is(false));
        assertThat(QueryValidator.validate("INSERT INTO test VALUES *"), is(false));
        assertThat(QueryValidator.validate("INSERT INTO test,test2 VALUES {}"), is(false));

        assertThat(QueryValidator.validate("INSERT INTO test VALUES {}"), is(true));
        assertThat(QueryValidator.validate("INSERT INTO test VALUES {\"a1\":\"1\"}"), is(true));
        assertThat(QueryValidator.validate("INSERT INTO test VALUES {\"a1\" : { \"a_2\":\"1\" }}"), is(true));
        assertThat(QueryValidator.validate("INSERT INTO test VALUES {\"a1\" : { \"a_2\": [\"1\", \"a_2\" ] }}"), is(true));
        assertThat(QueryValidator.validate("INSERT INTO test VALUES {\"a1\" : { \"a_2\": [{\"1\" : \"a_2\"} , {\"1_a\" : \"_a_2_.1\"} ] }}"), is(true));
    }

    @Test
    public void validateUpdateTest() {
        assertThat(QueryValidator.validate("UPDATE"), is(false));
        assertThat(QueryValidator.validate("UPDATE *"), is(false));
        assertThat(QueryValidator.validate("UPDATE test"), is(false));
        assertThat(QueryValidator.validate("UPDATE test SET"), is(false));
        assertThat(QueryValidator.validate("UPDATE test SET WHERE"), is(false));
        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 WHERE"), is(false));
        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 WHERE 1=1"), is(false));
        assertThat(QueryValidator.validate("UPDATE * SET x.a_1 = 1 WHERE 1=1"), is(false));

        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 = 1"), is(true));
        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 = '1'"), is(true));
        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 = y.a_1"), is(true));
        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 = 'y.a_1'"), is(true));
        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1"), is(true));
        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND 1=1"), is(true));
        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND 1=1 OR 1=1"), is(true));
        assertThat(QueryValidator.validate("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1"), is(true));
    }

    @Test
    public void validateDeleteTest() {
        assertThat(QueryValidator.validate("DELETE"), is(false));
        assertThat(QueryValidator.validate("DELETE FROM"), is(false));
        assertThat(QueryValidator.validate("DELETE FROM *"), is(false));
        assertThat(QueryValidator.validate("DELETE test FROM test"), is(false));
        assertThat(QueryValidator.validate("DELETE FROM test,test1"), is(false));

        assertThat(QueryValidator.validate("DELETE FROM test"), is(true));
        assertThat(QueryValidator.validate("DELETE FROM test WHERE 1=1"), is(true));
        assertThat(QueryValidator.validate("DELETE FROM test WHERE 1=1 AND 1=1"), is(true));
        assertThat(QueryValidator.validate("DELETE FROM test WHERE 1=1 AND 1=1 OR 1=1"), is(true));
        assertThat(QueryValidator.validate("DELETE FROM test WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1"), is(true));
    }

    @Test
    public void validateCreateIndexTest() {
        assertThat(QueryValidator.validate("CREATE INDEX"), is(false));
        assertThat(QueryValidator.validate("CREATE INDEX a_2.a_3"), is(false));
        assertThat(QueryValidator.validate("CREATE INDEX a_2.a_3 ON"), is(false));
        assertThat(QueryValidator.validate("CREATE INDEX a_2.a_3 ON a_2.a_3"), is(false));

        assertThat(QueryValidator.validate("CREATE INDEX a_2.a_3 ON a_2"), is(true));
    }

    @Test
    public void validateDropIndexTest() {
        assertThat(QueryValidator.validate("DROP INDEX"), is(false));
        assertThat(QueryValidator.validate("DROP INDEX a_2.a_3"), is(false));
        assertThat(QueryValidator.validate("DROP INDEX a_2.a_3 ON"), is(false));
        assertThat(QueryValidator.validate("DROP INDEX a_2.a_3 ON a_2.a_3"), is(false));

        assertThat(QueryValidator.validate("DROP INDEX a_2.a_3 ON a_2"), is(true));
    }

    @Test
    public void validateCreateTest() {
        assertThat(QueryValidator.validate("CREATE"), is(false));
        assertThat(QueryValidator.validate("CREATE *"), is(false));
        assertThat(QueryValidator.validate("CREATE a_2.a_3"), is(false));

        assertThat(QueryValidator.validate("CREATE a_2"), is(true));
    }

    @Test
    public void validateDropTest() {
        assertThat(QueryValidator.validate("DROP"), is(false));
        assertThat(QueryValidator.validate("DROP *"), is(false));
        assertThat(QueryValidator.validate("DROP a_2.a_3"), is(false));

        assertThat(QueryValidator.validate("DROP a_2"), is(true));
    }

    @Test
    public void validateJsonTest() {
        assertThat(QueryValidator.validateJson("{"), is(false));
        assertThat(QueryValidator.validateJson("{\"*"), is(false));
        assertThat(QueryValidator.validateJson("{\"a1\" : { \"a_2\":\"1\"}"), is(false));
        assertThat(QueryValidator.validateJson("{\"a\":\"b\":\"c\"}"), is(false));
        assertThat(QueryValidator.validateJson("{\"a\":\"b\",}"), is(false));
        assertThat(QueryValidator.validateJson("{\"a':\"b'}"), is(false));
        assertThat(QueryValidator.validateJson("{\"exp\":0,123}"), is(false));
        assertThat(QueryValidator.validateJson("{\"exp\":0,123}"), is(false));
        assertThat(QueryValidator.validateJson("{\"arr\":[1,\"abc\",]}"), is(false));
        assertThat(QueryValidator.validateJson("{\"arr\":[,\"abc\"]}"), is(false));

        assertThat(QueryValidator.validateJson("{}"), is(true));
        assertThat(QueryValidator.validateJson("{\"a1\" : { \"a_2\":\"1\" }}"), is(true));
        assertThat(QueryValidator.validateJson("{'a':'b'}"), is(true));
        assertThat(QueryValidator.validateJson("{\"a\":'b'}"), is(true));
        assertThat(QueryValidator.validateJson("{'a':\"b\"}"), is(true));
        assertThat(QueryValidator.validateJson("{\"null\":null}"), is(true));
        assertThat(QueryValidator.validateJson("{\"true\":true}"), is(true));
        assertThat(QueryValidator.validateJson("{\"false\":false}"), is(true));
        assertThat(QueryValidator.validateJson("{\"empty_object\":{}}"), is(true));
        assertThat(QueryValidator.validateJson("{\"empty\":\"\"}"), is(true));
        assertThat(QueryValidator.validateJson("{\"empty\":''}"), is(true));
        assertThat(QueryValidator.validateJson("{\"quote\":\"'\"}"), is(true));
        assertThat(QueryValidator.validateJson("{\"quote\":'\"'}"), is(true));
        assertThat(QueryValidator.validateJson("{\"quote\":\"'\"}"), is(true));
        assertThat(QueryValidator.validateJson("{\"quote\":\"\\\"\"}"), is(true));
        assertThat(QueryValidator.validateJson("{\"int\":123}"), is(true));
        assertThat(QueryValidator.validateJson("{\"int_neg\":-123}"), is(true));
        assertThat(QueryValidator.validateJson("{\"int_neg_null\":-0123}"), is(true));
        assertThat(QueryValidator.validateJson("{\"float\":-0.123}"), is(true));
        assertThat(QueryValidator.validateJson("{\"float\":-0.123}"), is(true));
        assertThat(QueryValidator.validateJson("{\"exp\":4.9e-123}"), is(true));
        assertThat(QueryValidator.validateJson("{\"exp\":4.9E-123}"), is(true));
        assertThat(QueryValidator.validateJson("{\"exp\":4.9e+123}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[[]]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[[],[]]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[{}]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[{},{}]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[[],{}]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[\"a\"]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":['a']}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[true]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[\"a\",\"b\"]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[1]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"arr\":[1,\"abc\"]}"), is(true));

        assertThat(QueryValidator.validateJson("{\"arr\":[1,\"abc\",true,-12.25,false,\"\",null,\"\\\"\",\"'\",'abc',[],{},{'a':'1'}]}"), is(true));
        assertThat(QueryValidator.validateJson("{\"a1\" : { \"arr\":[1,\"abc\",true,-12.25,false,\"\",null,\"\\\"\",\"'\",'abc'], \"b\":{ 'a3':'123'} }, \"b1\":\"\"}"), is(true));
        assertThat(QueryValidator.validateJson("{\"a1\" : { \"a_2\":\"1\", \"arr\":[1,\"abc\",true,-12.25,false,\"\",null,\"\\\"\",\"'\",'abc'], \"b\":2 }, \"b1\":\"\"}"), is(true));
    }
}
