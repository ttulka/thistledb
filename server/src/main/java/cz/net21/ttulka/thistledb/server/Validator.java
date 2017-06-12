package cz.net21.ttulka.thistledb.server;

import java.util.regex.Pattern;

/**
 * Created by ttulka
 */
// TODO
class Validator {

    private static final Pattern SELECT = Pattern.compile("SELECT\\s(\\*|[\\w\\d,\\s]+)\\sFROM\\s([\\w\\d]+)(\\sWHERE\\s([\\(\\)\\w\\d\\=\\s\\']+))?");
    private static final Pattern INSERT = Pattern.compile("INSERT INTO\\s([\\w\\d]+)\\sVALUES\\s(\\{.+\\})");

    private final String ql;

    public Validator(String ql) {
        this.ql = ql != null ? ql.toUpperCase() : "";
    }

    public boolean validate() {
        return SELECT.matcher(ql).matches() ||
               INSERT.matcher(ql).matches();
    }
}
