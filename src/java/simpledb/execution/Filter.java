package simpledb.execution;

import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param c The child operator
     */
    public Filter(Predicate p, OpIterator c) {
        this.predicate = p;
        this.child = c;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.child.open();
        super.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (this.child.hasNext()) {
            Tuple curr = this.child.next();
            if (this.predicate.filter(curr)) {
                return curr;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] children = { this.child };
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
