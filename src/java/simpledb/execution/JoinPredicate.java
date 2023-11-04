package simpledb.execution;

import java.io.Serializable;

import simpledb.storage.Tuple;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int fieldOneIndex;
    private final int fieldTwoIndex;

    private final Predicate.Op operator;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param f1 The field index into the first tuple in the predicate
     * @param f2 The field index into the second tuple in the predicate
     * @param op The operation to apply (as defined in Predicate.Op); either
     *           Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *           Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *           Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int f1, Predicate.Op op, int f2) {
        this.fieldOneIndex = f1;
        this.fieldTwoIndex = f2;
        this.operator = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        return t1.getField(this.fieldOneIndex).compare(this.operator, t2.getField(this.fieldTwoIndex));
    }

    public int getField1() {
        return this.fieldOneIndex;
    }

    public int getField2() {
        return this.fieldTwoIndex;
    }

    public Predicate.Op getOperator() {
        return this.operator;
    }
}
