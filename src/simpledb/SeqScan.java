package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
    private int tableid;
    private String tableAlias;
    private TransactionId tid;
    private DbFileIterator heapFileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        heapFileIterator = Database.getCatalog().getDbFile(tableid).iterator(tid);
    }

    public void open()
        throws DbException, TransactionAbortedException {
        heapFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc oldFieldTypes = Database.getCatalog().getTupleDesc(tableid);
        String[] oldFieldNames = oldFieldTypes.getFieldNames();
        String[] fieldNames = new String[oldFieldNames.length];
        for (int i = 0; i < oldFieldNames.length; i++){
            fieldNames[i] = tableAlias + "." + oldFieldNames[i];
        }
        return new TupleDesc(oldFieldTypes.getFieldTypes(), fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return heapFileIterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        return heapFileIterator.next();
    }

    public void close() {
        heapFileIterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        heapFileIterator.rewind();
    }
}
