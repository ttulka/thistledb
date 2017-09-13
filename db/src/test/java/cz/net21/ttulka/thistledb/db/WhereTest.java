package cz.net21.ttulka.thistledb.db;

import java.util.List;

import org.junit.Test;

import cz.net21.ttulka.thistledb.TestData;
import cz.net21.ttulka.thistledb.tson.TSONObject;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author ttulka
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
        Where.ConditionDataPart dataPart = Where.parseDataPart("person.name = \"John Smith\"");

        assertThat(dataPart, notNullValue());
        assertThat(dataPart.getKey(), is("person.name"));
        assertThat(dataPart.getValue(), is("John Smith"));
    }

    @Test
    public void parseDataPartWithQuotesTest() {
        Where.ConditionDataPart dataPart = Where.parseDataPart("person.name = \"John 'Mad dog' Smith\"");

        assertThat(dataPart, notNullValue());
        assertThat(dataPart.getKey(), is("person.name"));
        assertThat(dataPart.getValue(), is("John 'Mad dog' Smith"));

        dataPart = Where.parseDataPart("person.name = \"John O'Smith\"");

        assertThat(dataPart, notNullValue());
        assertThat(dataPart.getKey(), is("person.name"));
        assertThat(dataPart.getValue(), is("John O'Smith"));

        dataPart = Where.parseDataPart("person.name = 'John \"Mad dog\" Smith'");

        assertThat(dataPart, notNullValue());
        assertThat(dataPart.getKey(), is("person.name"));
        assertThat(dataPart.getValue(), is("John \"Mad dog\" Smith"));

        dataPart = Where.parseDataPart("quote = '\"'");

        assertThat(dataPart, notNullValue());
        assertThat(dataPart.getKey(), is("quote"));
        assertThat(dataPart.getValue(), is("\""));
    }

    @Test
    public void parseOrPartsTest() {
        List<Where.ConditionDataPart> orParts = Where.parseOrParts("person.name = \"John Smith\" OR person.age = 42");

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
                "name Like \"something*LIKE*you\" AND " +
                "name LIKE 'LIKE' AND " +
                "name like 'LIKE\"quote' AND " +
                "name like \"LIKE'apostrophe\"");

        assertThat(conditions, notNullValue());
        assertThat(conditions.size(), is(10));

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

        assertThat(conditions.get(7).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(7).getOrClause().get(0).getValue(), is("LIKE"));
        assertThat(conditions.get(7).getOrClause().get(0).getOperator(), is(Where.Operators.LIKE));

        assertThat(conditions.get(8).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(8).getOrClause().get(0).getValue(), is("LIKE\"quote"));
        assertThat(conditions.get(8).getOrClause().get(0).getOperator(), is(Where.Operators.LIKE));

        assertThat(conditions.get(9).getOrClause().get(0).getKey(), is("name"));
        assertThat(conditions.get(9).getOrClause().get(0).getValue(), is("LIKE'apostrophe"));
        assertThat(conditions.get(9).getOrClause().get(0).getOperator(), is(Where.Operators.LIKE));
    }

    @Test
    public void conditionValueMatchesTest() {
        TSONObject person = TestData.TSON_PERSON;

        Where.ConditionDataPart nameEqualsJohn = new Where.ConditionDataPart("person.name", Where.Operators.EQUAL, "John");
        assertThat(nameEqualsJohn.matches(person), is(true));

        Where.ConditionDataPart nameEqualsJohnCaseSensitive = new Where.ConditionDataPart("person.name", Where.Operators.EQUAL, "jOhN");
        assertThat(nameEqualsJohnCaseSensitive.matches(person), is(false));

        Where.ConditionDataPart nameEqualsPeter = new Where.ConditionDataPart("person.name", Where.Operators.EQUAL, "Peter");
        assertThat(nameEqualsPeter.matches(person), is(false));

        Where.ConditionDataPart nameNotEqualsJohn = new Where.ConditionDataPart("person.name", Where.Operators.NOT_EQUAL, "John");
        assertThat(nameNotEqualsJohn.matches(person), is(false));

        Where.ConditionDataPart nameNotEqualsPeter = new Where.ConditionDataPart("person.name", Where.Operators.NOT_EQUAL, "Peter");
        assertThat(nameNotEqualsPeter.matches(person), is(true));

        Where.ConditionDataPart nameGreaterThanA = new Where.ConditionDataPart("person.name", Where.Operators.GREATER, "A");
        assertThat(nameGreaterThanA.matches(person), is(true));

        Where.ConditionDataPart nameGreaterThanJohn = new Where.ConditionDataPart("person.name", Where.Operators.GREATER, "John");
        assertThat(nameGreaterThanJohn.matches(person), is(false));

        Where.ConditionDataPart nameGreaterOrEqualsJohn = new Where.ConditionDataPart("person.name", Where.Operators.GREATER_EQUAL, "John");
        assertThat(nameGreaterOrEqualsJohn.matches(person), is(true));

        Where.ConditionDataPart nameLessThanZ = new Where.ConditionDataPart("person.name", Where.Operators.LESS, "Z");
        assertThat(nameLessThanZ.matches(person), is(true));

        Where.ConditionDataPart nameLessThanJohn = new Where.ConditionDataPart("person.name", Where.Operators.LESS, "John");
        assertThat(nameLessThanJohn.matches(person), is(false));

        Where.ConditionDataPart nameLessOrEqualsJohn = new Where.ConditionDataPart("person.name", Where.Operators.LESS_EQUAL, "John");
        assertThat(nameLessOrEqualsJohn.matches(person), is(true));

        Where.ConditionDataPart nameLikeX = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "*");
        assertThat(nameLikeX.matches(person), is(true));

        Where.ConditionDataPart nameLikeJx = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "J*");
        assertThat(nameLikeJx.matches(person), is(true));

        Where.ConditionDataPart nameLikeXn = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "*n");
        assertThat(nameLikeXn.matches(person), is(true));

        Where.ConditionDataPart nameLikeJxn = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "J*n");
        assertThat(nameLikeJxn.matches(person), is(true));

        Where.ConditionDataPart nameLikeXoX = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "*o*");
        assertThat(nameLikeXoX.matches(person), is(true));

        Where.ConditionDataPart nameLikeJXoXhXn = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "J*o*h*n");
        assertThat(nameLikeJXoXhXn.matches(person), is(true));

        Where.ConditionDataPart nameLike_ohn = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "_ohn");
        assertThat(nameLike_ohn.matches(person), is(true));

        Where.ConditionDataPart nameLikeJoh_ = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "Joh_");
        assertThat(nameLikeJoh_.matches(person), is(true));

        Where.ConditionDataPart nameLikeJ_hn = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "J_hn");
        assertThat(nameLikeJ_hn.matches(person), is(true));

        Where.ConditionDataPart nameLikeJo_hn = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "Jo_hn");
        assertThat(nameLikeJo_hn.matches(person), is(false));

        Where.ConditionDataPart nameLike_o__ = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "_o__");
        assertThat(nameLike_o__.matches(person), is(true));

        Where.ConditionDataPart nameLikeYohn = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "?ohn");
        assertThat(nameLikeYohn.matches(person), is(true));

        Where.ConditionDataPart nameLikeYJohn = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "?John");
        assertThat(nameLikeYJohn.matches(person), is(true));

        Where.ConditionDataPart nameLikeYJohY = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "?Joh?");
        assertThat(nameLikeYJohY.matches(person), is(true));

        Where.ConditionDataPart nameLikeYJ_X = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "?J_*");
        assertThat(nameLikeYJ_X.matches(person), is(true));

        Where.ConditionDataPart nameLikeCaseInsensitive = new Where.ConditionDataPart("person.name", Where.Operators.LIKE, "jOhN");
        assertThat(nameLikeCaseInsensitive.matches(person), is(true));
    }
}
