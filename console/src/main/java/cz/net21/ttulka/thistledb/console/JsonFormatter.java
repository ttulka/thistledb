package cz.net21.ttulka.thistledb.console;

import java.io.PrintStream;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by ttulka
 */
class JsonFormatter {

    private final JSONObject json;
    private final PrintStream out;

    public JsonFormatter(JSONObject json, PrintStream out) {
        this.json = json;
        this.out = out;
    }

    public void formatJsonResult() {
        formatJsonResult(json, 0);
    }

    private void formatJsonResult(JSONObject json, int level) {
        printLevel(level, "{");
        Iterator<String> iterator = json.keys();
        boolean first = true;
        while (iterator.hasNext()) {
            String key = iterator.next();
            printLevel(level + 1, "\"" + key + "\" : ");

            formatJsonResult(json.get(key), level + 1);
            first = false;
        }
        printLevel(level, "}");
    }

    private void formatJsonResult(JSONArray array, int level) {
        printLevel(level, "[");
        Iterator<Object> iterator = array.iterator();
        while (iterator.hasNext()) {
            formatJsonResult(iterator.next(), level + 1);
        }
        printLevel(level, "]");
    }

    private void formatJsonResult(Object o, int level) {
        if (o instanceof JSONObject) {
            formatJsonResult((JSONObject) o, level + 1);
        } else if (o instanceof JSONArray) {
            formatJsonResult((JSONArray) o, level + 1);
        } else {
            printLevel(level + 1, "\"" + o + "\"");
        }
    }

    private void printLevel(int level, String str) {
        for (int i = 0; i < level; i++) {
            out.print("  ");
        }
        out.println(str);
    }
}
