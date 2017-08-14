package cz.net21.ttulka.thistledb.server.db;

import java.util.List;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by ttulka
 */
public class WhereTest {

    @Test
    public void upperCaseIgnoreAndOrInQuotesTest() {
        String where = "\"and and .here.is.and\" =\"atd and to or and\" anD person.name=\"John\"";

        String result = Where.upperCaseIgnoreAndOrInQuotes(where, "AND");

        assertThat("Should be deleted.", result, is("\"and and .here.is.and\" =\"atd and to or and\" AND person.name=\"John\""));
    }

    @Test
    public void parseDataPartTest() {
        Where.DataPart dataPart = Where.parseDataPart("person.name = \"John Smith\"");

        assertThat(dataPart, notNullValue());
        assertThat(dataPart.getKey(), is("person.name"));
        assertThat(dataPart.getValue(), is("John Smith"));
    }

    @Test
    public void parseOrPartsTest() {
        List<Where.DataPart> orParts = Where.parseOrParts("person.name = \"John Smith\" OR person.age = 42");

        assertThat(orParts, notNullValue());
        assertThat(orParts.size(), is(2));

        assertThat(orParts.get(0).getKey(), is("person.name"));
        assertThat(orParts.get(0).getValue(), is("John Smith"));

        assertThat(orParts.get(1).getKey(), is("person.age"));
        assertThat(orParts.get(1).getValue(), is("42"));
    }

    @Test
    public void parseAndPartsTest() {
        List<Where.Condition> andParts = Where.parseAndParts("person.name = \"John\" OR person.name = \"Johnny\" AND person.age = 42");

        assertThat(andParts, notNullValue());
        assertThat(andParts.size(), is(2));

        assertThat(andParts.get(0).getOrClause().size(), is(2));

        assertThat(andParts.get(0).getOrClause().get(0).getKey(), is("person.name"));
        assertThat(andParts.get(0).getOrClause().get(0).getValue(), is("John"));

        assertThat(andParts.get(0).getOrClause().get(1).getKey(), is("person.name"));
        assertThat(andParts.get(0).getOrClause().get(1).getValue(), is("Johnny"));

        assertThat(andParts.get(1).getOrClause().size(), is(1));

        assertThat(andParts.get(1).getOrClause().get(0).getKey(), is("person.age"));
        assertThat(andParts.get(1).getOrClause().get(0).getValue(), is("42"));
    }

    @Test
    public void matchesTest() {
        Where where = Where.create("person.name = \"John\" OR person.name = \"Johnny\" AND person.age = 42");

        assertThat(where.matches("{\"person\":{\"name\":\"John\",\"surname\":\"Smith\"}}"), is(false));
        assertThat(where.matches("{\"person\":{\"name\":\"John\",\"surname\":\"Smith\",\"age\":33}}"), is(false));
        assertThat(where.matches("{\"person\":{\"name\":\"Jon\",\"surname\":\"Smith\",\"age\":42}}"), is(false));
        assertThat(where.matches("{\"person\":{\"name\":\"Johnny\",\"surname\":\"Smith\",\"age\":42}}"), is(true));
    }

    @Test
    public void parseOperatorsTest() {
        List<Where.Condition> conditions = Where.parse(
                "name = \"Pe=ter\" AND " +
                "name != \"J!=ohn\" AND " +
                "name > \"A>b\" AND " +
                "name >= \"A>=b\" AND " +
                "name < \"Y<z\" AND " +
                "name <= \"Y<=z\" AND " +
                "name LIKE \"something*LIKE*you\"");

        assertThat(conditions, notNullValue());
        assertThat(conditions.size(), is(7));

        assertThat(conditions.get(0).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(0).getOrClause().get(0).getValue(), is("Pe=ter"));
        assertThat(conditions.get(0).getOrClause().get(0).getOperator(), is(Where.Operators.EQUAL));

        assertThat(conditions.get(1).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(1).getOrClause().get(0).getValue(), is("J!=ohn"));
        assertThat(conditions.get(1).getOrClause().get(0).getOperator(), is(Where.Operators.NOT_EQUAL));

        assertThat(conditions.get(2).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(2).getOrClause().get(0).getValue(), is("A>b"));
        assertThat(conditions.get(2).getOrClause().get(0).getOperator(), is(Where.Operators.GREATER));

        assertThat(conditions.get(3).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(3).getOrClause().get(0).getValue(), is("A>=b"));
        assertThat(conditions.get(3).getOrClause().get(0).getOperator(), is(Where.Operators.GREATER_EQUAL));

        assertThat(conditions.get(4).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(4).getOrClause().get(0).getValue(), is("Y<z"));
        assertThat(conditions.get(4).getOrClause().get(0).getOperator(), is(Where.Operators.LESS));

        assertThat(conditions.get(5).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(5).getOrClause().get(0).getValue(), is("Y<=z"));
        assertThat(conditions.get(5).getOrClause().get(0).getOperator(), is(Where.Operators.LESS_EQUAL));

        assertThat(conditions.get(6).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(6).getOrClause().get(0).getValue(), is("something*LIKE*you"));
        assertThat(conditions.get(6).getOrClause().get(0).getOperator(), is(Where.Operators.LIKE));
    }

    @Test
    public void conditionValueMatchesTest() {
        // TODO
    }
}
