package simpledb;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private int tupleNum;
    private PageId pid;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleNum = tupleno;
    }

    public int tupleno() {
        return tupleNum;
    }

    public PageId getPageId() {
        return pid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordId recordId = (RecordId) o;
        return tupleNum == recordId.tupleNum &&
                Objects.equals(pid, recordId.pid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tupleNum, pid);
    }
}
