package cz.net21.ttulka.thistledb.server;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class ValidatorTest {

    @Test
    public void validateSelectTest() {
        assertThat(new Validator(null).validate(), is(false));
        assertThat(new Validator("").validate(), is(false));
        assertThat(new Validator("xxx").validate(), is(false));
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
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE 1").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE 1 AND 1").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE 1 AND 1 OR 1").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE ((1 AND 1) OR 1)").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE ((1 AND 1) OR (1 OR 1))").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE ((1 AND 1) OR (1 OR 1)) AND 1").validate(), is(true));
        assertThat(new Validator("SELECT a1, a_2 FROM test WHERE ((a1=1 AND a_2 = '') OR (a1 = 'xxx' OR 1)) AND 1").validate(), is(true));
    }

    @Test
    public void validateInsertTest() {
        assertThat(new Validator(null).validate(), is(false));
        assertThat(new Validator("").validate(), is(false));
        assertThat(new Validator("xxx").validate(), is(false));
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
}
