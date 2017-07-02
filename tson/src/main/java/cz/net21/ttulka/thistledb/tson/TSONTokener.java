package cz.net21.ttulka.thistledb.tson;

import java.io.InputStream;
import java.io.Reader;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONTokener;

/**
 * Created by ttulka
 * <p>
 * JSON implementation keeping an order of elements.
 */
public class TSONTokener extends JSONTokener {

    /**
     * Construct a TSONTokener from a Reader.
     *
     * @param reader A reader.
     */
    public TSONTokener(Reader reader) {
        super(reader);
    }

    /**
     * Construct a TSONTokener from an InputStream.
     *
     * @param inputStream The source.
     */
    public TSONTokener(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Construct a TSONTokener from a string.
     *
     * @param s A source string.
     */
    public TSONTokener(String s) {
        super(s);
    }

    /**
     * Get the next value. The value can be a Boolean, Double, Integer,
     * JSONArray, JSONObject, Long, or String, or the JSONObject.NULL object.
     *
     * @return An object.
     * @throws JSONException If syntax error.
     */
    @Override
    public Object nextValue() throws JSONException {
        char c = this.nextClean();
        String string;

        switch (c) {
            case '"':
            case '\'':
                return this.nextString(c);
            case '{':
                this.back();
                return new TSONObject(this);
            case '[':
                this.back();
                return new JSONArray(this);
        }

        StringBuilder sb = new StringBuilder();
        while (c >= ' ' && ",:]}/\\\"[{;=#".indexOf(c) < 0) {
            sb.append(c);
            c = this.next();
        }
        this.back();

        string = sb.toString().trim();
        if ("".equals(string)) {
            throw this.syntaxError("Missing value");
        }
        return TSONObject.stringToValue(string);
    }
}
