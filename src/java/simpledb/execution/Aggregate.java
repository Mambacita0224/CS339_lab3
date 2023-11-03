package simpledb.execution;

import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private Aggregator aggregator;
    private int groupByFieldIndex;
    private int aggregateFieldIndex;
    private Aggregator.Op operator;
    private OpIterator aggregateIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param aField The column over which we are computing an aggregate.
     * @param gField The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param op     The aggregation operator to use
     */
    public Aggregate(OpIterator child, int aField, int gField, Aggregator.Op op) {
        this.child = child;
        this.groupByFieldIndex = gField;
        this.aggregateFieldIndex = aField;
        this.operator = op;

        Type aggregatorType = this.child.getTupleDesc().getFieldType(aField);
        Type groupByType = gField == Aggregator.NO_GROUPING ? null : this.child.getTupleDesc().getFieldType(gField);
        if (aggregatorType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gField, groupByType, aField, op);
        } else if (aggregatorType == Type.STRING_TYPE) {
            this.aggregator = new StringAggregator(gField, groupByType, aField, op);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.groupByFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        if (this.groupByFieldIndex == Aggregator.NO_GROUPING) {
            return null;
        }
        return this.child.getTupleDesc().getFieldName(this.groupByFieldIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.aggregateFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        return this.child.getTupleDesc().getFieldName(this.aggregateFieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        this.child.open();
        while (this.child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        }
        this.aggregateIterator = this.aggregator.iterator();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.aggregateIterator.hasNext()) {
            return this.aggregateIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if (this.aggregateIterator != null) {
            this.aggregateIterator.rewind();
        }
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        String aggregateColumnName = String.format("%s(%s)", this.operator, this.aggregateFieldName());
        if (this.groupByFieldIndex == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { aggregateColumnName });
        }
        Type groupByColumnType = this.child.getTupleDesc().getFieldType(this.groupByFieldIndex);
        return new TupleDesc(new Type[] { groupByColumnType, Type.INT_TYPE },
                new String[] { "groupValue", aggregateColumnName });
    }

    public void close() {
        super.close();
        this.child.close();
        this.aggregateIterator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
