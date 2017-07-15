package cz.net21.ttulka.thistledb.console;

import java.io.PrintStream;
import java.util.Iterator;

import org.json.JSONArray;

import cz.net21.ttulka.thistledb.tson.TSONObject;

/**
 * Created by ttulka
 */
class JsonFormatter {

    private final TSONObject json;
    private final PrintStream out;

    public JsonFormatter(String json, PrintStream out) {
        this.json = new TSONObject(json);
        this.out = out;
    }

    public void formatJsonResult(boolean last) {
        formatJsonResult(json, 1, last);
    }

    private void formatJsonResult(TSONObject json, int level, boolean last) {
        out.println("{");
        Iterator<String> iterator = json.keys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            printLevel(level + 1, "\"" + key + "\" : ");

            formatJsonResult(json.get(key), level + 1, false, !iterator.hasNext());
        }
        printLevel(level - 1, "}");
        if (!last) {
            out.print(", ");
        }
        out.println();
    }

    private void formatJsonResult(JSONArray array, int level, boolean last) {
        out.println("[");
        Iterator<Object> iterator = array.iterator();
        while (iterator.hasNext()) {
            formatJsonResult(iterator.next(), level + 1, true, !iterator.hasNext());
        }
        printLevel(level - 1, "]");
        if (!last) {
            out.print(", ");
        }
        out.println();
    }

    private void formatJsonResult(Object o, int level, boolean printLevel, boolean last) {
        if (printLevel) {
            printLevel(level, "");
        }

        if (o instanceof TSONObject) {
            formatJsonResult((TSONObject) o, level + 1, last);
        } else if (o instanceof JSONArray) {
            formatJsonResult((JSONArray) o, level + 1, last);
        } else {
            if (o instanceof String) {
                out.print("\"" + o + "\"");
            } else {
                out.print(o);
            }
            if (!last) {
                out.print(", ");
            }
            out.println();
        }
    }

    private void printLevel(int level, String str) {
        for (int i = 0; i < level; i++) {
            out.print("  ");
        }
        out.print(str);
    }
}
