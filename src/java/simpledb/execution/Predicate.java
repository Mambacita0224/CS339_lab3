package simpledb.execution;

import java.io.Serializable;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constants used for return codes in Field.compare
     */

    public final int field_number;

    public final Op operation;

    public final Field operand;

    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    /**
     * Constructor.
     *
     * @param field   field number of passed in tuples to compare against.
     * @param op      operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        this.field_number = field;
        this.operation = op;
        this.operand = operand;
        // TODO: some code goes here
    }

    /**
     * @return the field number
     */
    public int getField() {
        return this.field_number;
        // TODO: some code goes here
        // return -1;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        return this.operation;
        // TODO: some code goes here
        // return null;
    }

    /**
     * @return the operand
     */
    public Field getOperand() {
        return this.operand;
        // TODO: some code goes here
        // return null;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        return t.getField(this.getField()).compare(this.getOp(), this.getOperand());
        // TODO: some code goes here
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        return String.format("fieldId: %s, operator: %s, operand: %s", this.field_number, this.operation, this.operand);
    }
}
