package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
    private Type[] fieldTypes;
    private String[] fieldNames;

    public Type[] getFieldTypes() {
        return fieldTypes;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldTypes(Type[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        TupleDesc tupleDesc = new TupleDesc();
        Type[] fieldTypes = new Type[td1.numFields() + td2.numFields()];
        String[] fieldNames = new String[td1.numFields() + td2.numFields()];

        int j = 0;
        for (int i = 0; i < td1.numFields(); i++, j++) {
            fieldTypes[j] = td1.getFieldTypes()[i];
            fieldNames[j] = td1.getFieldNames()[i];
        }
        for (int i = 0; i < td2.numFields(); i++, j++) {
            fieldTypes[j] = td2.getFieldTypes()[i];
            fieldNames[j] = td2.getFieldNames()[i];
        }

        tupleDesc.setFieldNames(fieldNames);
        tupleDesc.setFieldTypes(fieldTypes);

        return tupleDesc;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        fieldTypes = typeAr;
        fieldNames = fieldAr;
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        fieldTypes = typeAr;
        fieldNames = new String[typeAr.length];
    }

    /**
     * Constructor.
     * Create a new tuple desc
     */
    public TupleDesc() {
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return fieldTypes.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i > fieldNames.length) {
            throw new NoSuchElementException();
        } else {
            return fieldNames[i];
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < fieldNames.length; i++) {
            if (name.equals(fieldNames[i])) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        if (i > fieldTypes.length) {
            throw new NoSuchElementException();
        } else {
            return fieldTypes[i];
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (int i = 0; i < fieldTypes.length; i++) {
            size += fieldTypes[i].getLen();
        }
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        if (this.getSize() != ((TupleDesc) o).getSize() || this.numFields() != ((TupleDesc) o).numFields()) {
            return false;
        }

        for (int i = 0; i < numFields(); i++) {
            if (fieldTypes[i] != ((TupleDesc) o).getFieldTypes()[i]) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields(); i++) {
            sb.append(fieldTypes[i] + "(" + fieldNames[i] + "),");
        }
        return sb.substring(0, sb.length() - 1);
    }
}
