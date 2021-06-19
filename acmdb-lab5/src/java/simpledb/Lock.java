package simpledb;

import java.util.ArrayList;
import java.util.HashSet;

public class Lock {
    public PageId pid;
    private HashSet<TransactionId> sharedLockHolders;
    private HashSet<TransactionId> exLockHolders;
    private HashSet<TransactionId> exCandidates;
    private HashSet<TransactionId> sharedCandidates;

    private boolean shared;

    public Lock(PageId pid, boolean shared) {
        this.pid = pid;
        this.shared = shared;
        this.sharedLockHolders = new HashSet<>();
        this.exLockHolders = new HashSet<>();
        this.exCandidates = new HashSet<>();
        this.sharedCandidates = new HashSet<>();
    }

    public PageId getPid() {
        return pid;
    }

    public boolean isShared() {
        return shared;
    }

    public HashSet<TransactionId> getExLockHolders() {
        return exLockHolders;
    }

    public HashSet<TransactionId> getSharedLockHolders() {
        return sharedLockHolders;
    }

    public HashSet<TransactionId> getExCandidates() {
        return exCandidates;
    }

    public HashSet<TransactionId> getSharedCandidates() {
        return sharedCandidates;
    }

    public void setShared(boolean shared) {
        this.shared = shared;
    }
}
