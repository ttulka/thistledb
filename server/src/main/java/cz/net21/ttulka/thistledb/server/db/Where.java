package cz.net21.ttulka.thistledb.server.db;

/**
 * Created by ttulka
 * <p>
 * Represent a where clause.
 */
class Where {

    public Where(String where) {
        // TODO
    }

    public boolean matches(String json) {
        if (json == null) {
            return false;
        }
        // TODO
        return true;
    }
}
