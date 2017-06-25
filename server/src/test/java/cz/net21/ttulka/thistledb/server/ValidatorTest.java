package cz.net21.ttulka.thistledb.server;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class ValidatorTest {

    @Test
    public void validateInsertWrongTest() {
        assertThat(new Validator(null).validate(), is(false));
        assertThat(new Validator("").validate(), is(false));
        assertThat(new Validator("xxx").validate(), is(false));
    }

    @Test
    public void validateSelectTest() {
        assertThat(new Validator("SELECT").validate(), is(false));
        assertThat(new Validator("SELECT * FROM").validate(), is(false));
        assertThat(new Validator("SELECT FROM").validate(), is(false));
        assertThat(new Validator("SELECT FROM test").validate(), is(false));
        assertThat(new Validator("FROM * SELECT test").validate(), is(false));
        assertThat(new Validator("SELECT * FROM test WHERE").validate(), is(false));
        assertThat(new Validator("SELECT * FROM test,test2 WHERE 1").validate(), is(false));

        assertThat(new Validator("SELECT * FROM test").validate(), is(true));
        assertThat(new Validator("SELECT a1 FROM test").validate(), is(true));
        assertThat(new Validator("SELECT a1,a_2 FROM test").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE 1=1").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE 1=1 AND 1=1").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE 1=1 AND 1=1 OR 1=1").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1").validate(), is(true));
    }

    @Test
    public void validateInsertTest() {
        assertThat(new Validator("INSERT").validate(), is(false));
        assertThat(new Validator("INSERT INTO").validate(), is(false));
        assertThat(new Validator("INSERT INTO test").validate(), is(false));
        assertThat(new Validator("INSERT INTO test VALUES").validate(), is(false));
        assertThat(new Validator("INSERT INTO test VALUES *").validate(), is(false));
        assertThat(new Validator("INSERT INTO test,test2 VALUES {}").validate(), is(false));

        assertThat(new Validator("INSERT INTO test VALUES {\"a1\":\"1\"}").validate(), is(true));
        assertThat(new Validator("INSERT INTO test VALUES {\"a1\" : { \"a_2\":\"1\" }}").validate(), is(true));
        assertThat(new Validator("INSERT INTO test VALUES {\"a1\" : { \"a_2\": [\"1\", \"a_2\" ] }}").validate(), is(true));
        assertThat(new Validator("INSERT INTO test VALUES {\"a1\" : { \"a_2\": [{\"1\" : \"a_2\"} , {\"1_a\" : \"_a_2_.1\"} ] }}").validate(), is(true));
    }

    @Test
    public void validateUpdateTest() {
        assertThat(new Validator("UPDATE").validate(), is(false));
        assertThat(new Validator("UPDATE *").validate(), is(false));
        assertThat(new Validator("UPDATE test").validate(), is(false));
        assertThat(new Validator("UPDATE test SET").validate(), is(false));
        assertThat(new Validator("UPDATE test SET WHERE").validate(), is(false));
        assertThat(new Validator("UPDATE test SET x.a_1 WHERE").validate(), is(false));
        assertThat(new Validator("UPDATE test SET x.a_1 WHERE 1=1").validate(), is(false));
        assertThat(new Validator("UPDATE test SET x.a_1 = WHERE 1=1").validate(), is(false));
        assertThat(new Validator("UPDATE * SET x.a_1 = 1 WHERE 1=1").validate(), is(false));

        assertThat(new Validator("UPDATE test SET x.a_1 = 1").validate(), is(true));
        assertThat(new Validator("UPDATE test SET x.a_1 = '1'").validate(), is(true));
        assertThat(new Validator("UPDATE test SET x.a_1 = y.a_1").validate(), is(true));
        assertThat(new Validator("UPDATE test SET x.a_1 = 'y.a_1'").validate(), is(true));
        assertThat(new Validator("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1").validate(), is(true));
        assertThat(new Validator("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND 1=1").validate(), is(true));
        assertThat(new Validator("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND 1=1 OR 1=1").validate(), is(true));
        assertThat(new Validator("UPDATE test SET x.a_1 = 'y.a_1' WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1").validate(), is(true));
    }

    @Test
    public void validateDeleteTest() {
        assertThat(new Validator("DELETE").validate(), is(false));
        assertThat(new Validator("DELETE FROM").validate(), is(false));
        assertThat(new Validator("DELETE FROM *").validate(), is(false));
        assertThat(new Validator("DELETE test FROM test").validate(), is(false));
        assertThat(new Validator("DELETE FROM test,test1").validate(), is(false));

        assertThat(new Validator("DELETE FROM test").validate(), is(true));
        assertThat(new Validator("DELETE FROM test WHERE 1=1").validate(), is(true));
        assertThat(new Validator("DELETE FROM test WHERE 1=1 AND 1=1").validate(), is(true));
        assertThat(new Validator("DELETE FROM test WHERE 1=1 AND 1=1 OR 1=1").validate(), is(true));
        assertThat(new Validator("DELETE FROM test WHERE 1=1 AND a_2 = '' OR a1 = 'xxx' OR 1=1 AND 1=1").validate(), is(true));
    }

    @Test
    public void validateCreateIndexTest() {
        assertThat(new Validator("CREATE INDEX").validate(), is(false));
        assertThat(new Validator("CREATE INDEX a_2.a_3").validate(), is(false));
        assertThat(new Validator("CREATE INDEX a_2.a_3 ON").validate(), is(false));
        assertThat(new Validator("CREATE INDEX a_2.a_3 ON a_2.a_3").validate(), is(false));

        assertThat(new Validator("CREATE INDEX a_2.a_3 ON a_2").validate(), is(true));
    }

    @Test
    public void validateDropIndexTest() {
        assertThat(new Validator("DROP INDEX").validate(), is(false));
        assertThat(new Validator("DROP INDEX a_2.a_3").validate(), is(false));
        assertThat(new Validator("DROP INDEX a_2.a_3 ON").validate(), is(false));
        assertThat(new Validator("DROP INDEX a_2.a_3 ON a_2.a_3").validate(), is(false));

        assertThat(new Validator("DROP INDEX a_2.a_3 ON a_2").validate(), is(true));
    }

    @Test
    public void validateCreateTest() {
        assertThat(new Validator("CREATE").validate(), is(false));
        assertThat(new Validator("CREATE *").validate(), is(false));
        assertThat(new Validator("CREATE a_2.a_3").validate(), is(false));

        assertThat(new Validator("CREATE a_2").validate(), is(false));
    }

    @Test
    public void validateDropTest() {
        assertThat(new Validator("DROP").validate(), is(false));
        assertThat(new Validator("DROP *").validate(), is(false));
        assertThat(new Validator("DROP a_2.a_3").validate(), is(false));

        assertThat(new Validator("DROP a_2").validate(), is(false));
    }
}
