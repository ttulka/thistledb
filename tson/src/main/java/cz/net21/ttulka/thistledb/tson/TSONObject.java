package cz.net21.ttulka.thistledb.tson;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONTokener;

/**
 * JSON implementation keeping an order of elements.
 * <p>
 * @author ttulka
 */
public class TSONObject extends JSONObject {

    /**
     * The map where the TSONObject's properties are kept.
     */
    private final Map<String, Object> map;

    /**
     * Construct an empty TSONObject.
     */
    public TSONObject() {
        this.map = new LinkedHashMap<>();
    }

    /**
     * Construct a TSONObject from a source JSON text string. This is the most
     * commonly used TSONObject constructor.
     *
     * @param source A string beginning with <code>{</code>&nbsp;<small>(left brace)</small> and ending with <code>}</code> &nbsp;<small>(right brace)</small>.
     * @throws JSONException If there is a syntax error in the source string or a duplicated key.
     */
    public TSONObject(String source) throws JSONException {
        this(new JSONTokener(source) {
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
        });
    }

    /**
     * Construct a JSONObject from a JSONTokener.
     *
     * @param x A JSONTokener object containing the source string.
     * @throws JSONException If there is a syntax error in the source string or a duplicated key.
     */
    private TSONObject(JSONTokener x) throws JSONException {
        this();
        char c;
        String key;

        if (x.nextClean() != '{') {
            throw x.syntaxError("A JSONObject text must begin with '{'");
        }
        for (; ; ) {
            c = x.nextClean();
            switch (c) {
                case 0:
                    throw x.syntaxError("A JSONObject text must end with '}'");
                case '}':
                    return;
                default:
                    x.back();
                    key = x.nextValue().toString();
            }

            c = x.nextClean();
            if (c != ':') {
                throw x.syntaxError("Expected a ':' after a key");
            }
            this.putOnce(key, x.nextValue());

            switch (x.nextClean()) {
                case ';':
                case ',':
                    if (x.nextClean() == '}') {
                        return;
                    }
                    x.back();
                    break;
                case '}':
                    return;
                default:
                    throw x.syntaxError("Expected a ',' or '}'");
            }
        }
    }

    /**
     * Determine if the JSONObject contains a specific key.
     *
     * @param key A key string.
     * @return true if the key exists in the JSONObject.
     */
    @Override
    public boolean has(String key) {
        return this.map.containsKey(key);
    }

    /**
     * Get a set of keys of the JSONObject. Modifying this key Set will also modify the
     * JSONObject. Use with caution.
     *
     * @return A keySet.
     * @see Map#keySet()
     */
    @Override
    public Set<String> keySet() {
        return this.map.keySet();
    }

    /**
     * Get a set of entries of the JSONObject. These are raw values and may not
     * match what is returned by the JSONObject get* and opt* functions. Modifying
     * the returned EntrySet or the Entry objects contained therein will modify the
     * backing JSONObject. This does not return a clone or a read-only view.
     * <p>
     * Use with caution.
     *
     * @return An Entry Set
     * @see Map#entrySet()
     */
    protected Set<Entry<String, Object>> entrySet() {
        return this.map.entrySet();
    }

    /**
     * Get the number of keys stored in the JSONObject.
     *
     * @return The number of keys in the JSONObject.
     */
    @Override
    public int length() {
        return this.map.size();
    }

    /**
     * Produce a JSONArray containing the names of the elements of this
     * JSONObject.
     *
     * @return A JSONArray containing the key strings, or null if the JSONObject is empty.
     */
    @Override
    public JSONArray names() {
        if (this.map.isEmpty()) {
            return null;
        }
        return new JSONArray(this.map.keySet());
    }

    /**
     * Get an optional value associated with a key.
     *
     * @param key A key string.
     * @return An object which is the value, or null if there is no value.
     */
    public Object opt(String key) {
        return key == null ? null : this.map.get(key);
    }

    /**
     * Put a key/boolean pair in the JSONObject.
     *
     * @param key   A key string.
     * @param value A boolean which is the value.
     * @return this.
     * @throws JSONException If the key is null.
     */
    public JSONObject put(String key, boolean value) throws JSONException {
        this.put(key, value ? Boolean.TRUE : Boolean.FALSE);
        return this;
    }

    /**
     * Put a key/value pair in the JSONObject. If the value is null, then the
     * key will be removed from the JSONObject if it is present.
     *
     * @param key   A key string.
     * @param value An object which is the value. It should be of one of these types: Boolean, Double, Integer, JSONArray, JSONObject, Long, String, or the
     *              JSONObject.NULL object.
     * @return this.
     * @throws JSONException If the value is non-finite number or if the key is null.
     */
    @Override
    public JSONObject put(String key, Object value) throws JSONException {
        if (key == null) {
            throw new NullPointerException("Null key.");
        }
        if (value != null) {
            testValidity(value);
            this.map.put(key, value);
        } else {
            this.remove(key);
        }
        return this;
    }

    /**
     * Remove a name and its value, if present.
     *
     * @param key The name to be removed.
     * @return The value that was associated with the name, or null if there was no value.
     */
    @Override
    public Object remove(String key) {
        return this.map.remove(key);
    }

    /**
     * Write the contents of the JSONObject as JSON text to a writer. For
     * compactness, no whitespace is added.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return The writer.
     * @throws JSONException
     */
    public Writer write(Writer writer) throws JSONException {
        return this.write(writer, 0, 0);
    }

    static final Writer writeValue(Writer writer, Object value,
                                   int indentFactor, int indent) throws JSONException, IOException {
        if (value == null || value.equals(null)) {
            writer.write("null");
        } else if (value instanceof JSONString) {
            Object o;
            try {
                o = ((JSONString) value).toJSONString();
            } catch (Exception e) {
                throw new JSONException(e);
            }
            writer.write(o != null ? o.toString() : quote(value.toString()));
        } else if (value instanceof Number) {
            // not all Numbers may match actual JSON Numbers. i.e. fractions or Imaginary
            final String numberAsString = numberToString((Number) value);
            try {
                // Use the BigDecimal constructor for it's parser to validate the format.
                @SuppressWarnings("unused")
                BigDecimal testNum = new BigDecimal(numberAsString);
                // Close enough to a JSON number that we will use it unquoted
                writer.write(numberAsString);
            } catch (NumberFormatException ex) {
                // The Number value is not a valid JSON number.
                // Instead we will quote it as a string
                quote(numberAsString, writer);
            }
        } else if (value instanceof Boolean) {
            writer.write(value.toString());
        } else if (value instanceof Enum<?>) {
            writer.write(quote(((Enum<?>) value).name()));
        } else if (value instanceof JSONObject) {
            ((JSONObject) value).write(writer, indentFactor, indent);
        } else if (value instanceof JSONArray) {
            ((JSONArray) value).write(writer, indentFactor, indent);
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            new JSONObject(map).write(writer, indentFactor, indent);
        } else if (value instanceof Collection) {
            Collection<?> coll = (Collection<?>) value;
            new JSONArray(coll).write(writer, indentFactor, indent);
        } else if (value.getClass().isArray()) {
            new JSONArray(value).write(writer, indentFactor, indent);
        } else {
            quote(value.toString(), writer);
        }
        return writer;
    }

    static final void indent(Writer writer, int indent) throws IOException {
        for (int i = 0; i < indent; i += 1) {
            writer.write(' ');
        }
    }

    /**
     * Write the contents of the JSONObject as JSON text to a writer.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @param writer       Writes the serialized JSON
     * @param indentFactor The number of spaces to add to each level of indentation.
     * @param indent       The indentation of the top level.
     * @return The writer.
     * @throws JSONException
     */
    @Override
    public Writer write(Writer writer, int indentFactor, int indent)
            throws JSONException {
        try {
            boolean commanate = false;
            final int length = this.length();
            writer.write('{');

            if (length == 1) {
                final Entry<String, ?> entry = this.entrySet().iterator().next();
                writer.write(quote(entry.getKey()));
                writer.write(':');
                if (indentFactor > 0) {
                    writer.write(' ');
                }
                writeValue(writer, entry.getValue(), indentFactor, indent);
            } else if (length != 0) {
                final int newindent = indent + indentFactor;
                for (final Entry<String, ?> entry : this.entrySet()) {
                    if (commanate) {
                        writer.write(',');
                    }
                    if (indentFactor > 0) {
                        writer.write('\n');
                    }
                    indent(writer, newindent);
                    writer.write(quote(entry.getKey()));
                    writer.write(':');
                    if (indentFactor > 0) {
                        writer.write(' ');
                    }
                    writeValue(writer, entry.getValue(), indentFactor, newindent);
                    commanate = true;
                }
                if (indentFactor > 0) {
                    writer.write('\n');
                }
                indent(writer, indent);
            }
            writer.write('}');
            return writer;
        } catch (IOException exception) {
            throw new JSONException(exception);
        }
    }

    /**
     * Returns a java.util.Map containing all of the entries in this object.
     * If an entry in the object is a JSONArray or JSONObject it will also
     * be converted.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return a java.util.Map containing the entries of this object
     */
    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        for (Entry<String, Object> entry : this.entrySet()) {
            Object value;
            if (entry.getValue() == null || NULL.equals(entry.getValue())) {
                value = null;
            } else if (entry.getValue() instanceof JSONObject) {
                value = ((JSONObject) entry.getValue()).toMap();
            } else if (entry.getValue() instanceof JSONArray) {
                value = ((JSONArray) entry.getValue()).toList();
            } else {
                value = entry.getValue();
            }
            results.put(entry.getKey(), value);
        }
        return results;
    }

    /**
     * Finds a sub JSON object by the path.
     *
     * @param path the path with comma-separated levels, eg.: "addressBook.person.name"
     * @return the sub object or null
     */
    public Object findByPath(String path) {
        if (path == null) {
            return null;
        }
        TSONObject json = this;
        Object toReturn = null;

        for (String keyPart : path.split("\\.")) {
            if (json == null || !json.keySet().contains(keyPart)) {
                return null;
            }
            toReturn = json.get(keyPart);

            if (toReturn instanceof TSONObject) {
                json = (TSONObject) toReturn;
            } else {
                json = null;
            }
        }
        return toReturn;
    }

    /**
     * Set a sub JSON object by the path.
     *
     * @param path  the path with comma-separated levels, eg.: "addressBook.person.name"
     * @param value the sub-object to set
     * @return true if the sub-object was changed, otherwise false
     */
    public TSONObject updateByPath(String path, Object value) {
        TSONObject json = new TSONObject(this.toString());
        TSONObject ref = json;

        if (path == null) {
            return json;
        }

        String[] splittedPath = path.split("\\.");

        int index = 0;
        for (String keyPart : splittedPath) {
            if (json == null || !json.keySet().contains(keyPart)) {
                break;
            }
            Object o = json.get(keyPart);

            if (o instanceof TSONObject) {
                json = (TSONObject) o;
            } else {
                break;
            }
            index ++;
        }

        String[] restPath = Arrays.copyOfRange(splittedPath, index + 1, splittedPath.length);
        json.put(splittedPath[index], createSubElement(restPath, value));

        return ref;
    }

    private Object createSubElement(String[] path, Object value) {
        for (int i = path.length - 1; i >= 0; i--) {
            TSONObject json = new TSONObject();
            json.put(path[i], value);
            value = json;
        }
        return value;
    }
}
