package cz.net21.ttulka.thistledb.server;

import java.util.regex.Pattern;

/**
 * Created by ttulka
 */
// TODO
class Validator {

    static final Pattern SELECT = Pattern.compile("SELECT\\s(\\*|[\\w\\d,\\s]+)\\sFROM\\s([\\w\\d]+)(\\sWHERE\\s([()\\w\\d=\\s']+))?");
    static final Pattern INSERT = Pattern.compile("INSERT\\sINTO\\s([\\w\\d]+)\\sVALUES\\s(\\{.+\\})");
    static final Pattern UPDATE = Pattern.compile("UPDATE\\s([\\w\\d]+)\\sSET\\s(((?!WHERE).)+)(\\s)?=(\\s)?(((?!WHERE).)+)(\\sWHERE\\s([()\\w\\d=\\s']+))?");
    static final Pattern DELETE = Pattern.compile("DELETE\\sFROM\\s([\\w\\d]+)\\s(WHERE\\s([()\\w\\d=\\s\']+))?");
    static final Pattern CREATE = Pattern.compile("CREATE\\s^(INDEX)([\\w\\d]+)");
    static final Pattern DROP = Pattern.compile("DROP\\s^(INDEX)([\\w\\d]+)");
    static final Pattern CREATE_INDEX = Pattern.compile("CREATE\\sINDEX\\s(.+)\\sON\\s([\\w\\d]+)");
    static final Pattern DROP_INDEX = Pattern.compile("DROP\\sINDEX\\s(.+)\\sON\\s([\\w\\d]+)");

    private final String ql;

    public Validator(String ql) {
        this.ql = ql != null ? ql.toUpperCase() : "";
    }

    public boolean validate() {
        return SELECT.matcher(ql).matches() ||
               INSERT.matcher(ql).matches() ||
               UPDATE.matcher(ql).matches() ||
               DELETE.matcher(ql).matches() ||
               CREATE.matcher(ql).matches() ||
               DROP.matcher(ql).matches() ||
               CREATE_INDEX.matcher(ql).matches() ||
               DROP_INDEX.matcher(ql).matches();
    }
}
