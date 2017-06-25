package cz.net21.ttulka.thistledb.server;

import java.util.regex.Pattern;

/**
 * Created by ttulka
 */
// TODO
class Validator {

    private static final String COLLECTION = "[\\w\\d]+";

    private static final String JSON = "\\{.+\\}";    // TODO

    private static final String JSON_PATH = "[\\w\\d\\._\\-$]+";    // TODO
    private static final String JSON_PATH_COMPOSITED = "((" + JSON_PATH + ")(\\s*,\\s*)?)+";

    private static final String WHERE = "(" + JSON_PATH + "(\\s*)=(\\s*+)(('(.+)')|(\\d+)))+";
    private static final String WHERE_COMPOSITED = "(" + WHERE + ")(\\s+(AND|OR)\\s+(" + WHERE + "))*";

    static final Pattern SELECT = Pattern.compile("SELECT(\\s+)(\\*|" + JSON_PATH_COMPOSITED + ")(\\s+)FROM(\\s+)(" + COLLECTION + ")((\\s+)WHERE(\\s+)(" + WHERE_COMPOSITED + "))?");
    static final Pattern INSERT = Pattern.compile("INSERT(\\s+)INTO(\\s+)(" + COLLECTION + ")(\\s+)VALUES(\\s+)(" + JSON + ")");
    static final Pattern UPDATE = Pattern.compile("UPDATE(\\s+)(" + COLLECTION + ")(\\s+)SET((\\s+)((?!WHERE).)+)(\\s*)=(\\s*)(((?!WHERE).)+)((\\s+)WHERE(\\s+)(" + WHERE_COMPOSITED + "))?");
    static final Pattern DELETE = Pattern.compile("DELETE(\\s+)FROM(\\s+)(" + COLLECTION + ")((\\s+)WHERE(\\s+)(" + WHERE_COMPOSITED + "))?");
    static final Pattern CREATE = Pattern.compile("CREATE(\\s+)^(INDEX)(" + COLLECTION + ")");
    static final Pattern DROP = Pattern.compile("DROP(\\s+)^(INDEX)(" + COLLECTION + ")");
    static final Pattern CREATE_INDEX = Pattern.compile("CREATE(\\s+)INDEX(\\s+)(" + JSON_PATH + ")(\\s+)ON(\\s+)(" + COLLECTION + ")");
    static final Pattern DROP_INDEX = Pattern.compile("DROP(\\s+)INDEX(\\s+)(" + JSON_PATH + ")(\\s+)ON(\\s+)(" + COLLECTION + ")");

    private final String ql;

    public Validator(String ql) {
        this.ql = ql != null ? ql.trim().toUpperCase() : "";
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
