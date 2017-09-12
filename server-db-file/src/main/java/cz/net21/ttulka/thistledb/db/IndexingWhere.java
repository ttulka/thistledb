package cz.net21.ttulka.thistledb.db;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Analyses the WHERE condition and finds indexes based on it.
 *
 * @author ttulka
 */
class IndexingWhere {

    private boolean isIndexed = false;

    private Iterator<Long> positions = null;

    public IndexingWhere(Where where, Indexing indexing) {
        Set<Long> wherePositions = null;

        for (Where.Condition and : where.getAndConditions()) {
            // all OR conditions must be indexed
            boolean andIndexed = true;
            Set<Long> andPositions = new HashSet<>();

            for (Where.ConditionDataPart or : and.getOrClause()) {
                if (!or.getOperator().equals(Where.Operators.EQUAL)) {
                    andIndexed = false;
                    break;
                }
                if (!indexing.exists(or.getKey())) {
                    andIndexed = false;
                    break;
                }
                Set<Long> orPositions = indexing.positions(or.getKey(), or.getValue());
                if (orPositions != null) {
                    andPositions.addAll(orPositions);
                }
            }

            if (andIndexed) {
                isIndexed = true;   // at least one AND must be indexed

                if (wherePositions == null) {
                    wherePositions = new HashSet<>(andPositions);
                } else {
                    wherePositions.retainAll(andPositions);
                }

                // if any AND condition makes the result empty, makes no sense to continue
                if (wherePositions.isEmpty()) {
                    break;
                }
            }
        }

        if (wherePositions != null) {
            positions = createIterator(wherePositions);
        }
    }

    private Iterator<Long> createIterator(Set<Long> positions) {
        return new Iterator<Long>() {

            private Iterator<Long> iterator = positions.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Long next() {
                return iterator.next();
            }
        };
    }

    /**
     * @return true if the condition is indexed, otherwise false.
     */
    public boolean isIndexed() {
        return isIndexed;
    }

    /**
     * @return the next position in the collection file, or -1 if there is no other record.
     */
    public long nextPosition() {
        if (!isIndexed) {
            throw new IllegalStateException("Cannot call nextPosition() when not indexed.");
        }
        if (positions.hasNext()) {
            return positions.next();
        }
        return -1;
    }
}
