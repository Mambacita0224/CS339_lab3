package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private OpIterator child;
    private int tableId;

    private boolean inserted;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we
     *                     are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        this.inserted = false;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        super.open();
    }

    public void close() {
        super.close();
        this.child.close();
        this.inserted = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        this.inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (inserted) {
            return null;
        }
        int insertCount = 0;
        while (this.child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(this.transactionId, this.tableId, this.child.next());
                insertCount++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        Tuple returnTuple = new Tuple(this.getTupleDesc());
        returnTuple.setField(insertCount, new IntField(insertCount));
        return returnTuple;
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
