package beehive.validator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import beehive.validator.util.*;

public class Validator {

    // nodeid->ts of the latest writer
    private ConcurrentHashMap<String, Long> conflictTable;
    private ConcurrentHashMap<String, NodeRWLock> cTable;

    private List<ReentrantLock> cTableLockList;
    //public Config configParams=null;
    private int nLocks = 256;

    public Validator() {
        //configParams = Config.getInstance();
        //nLocks = configParams.getIntValue("ConcurrentValidator.nLocks");
        conflictTable = new ConcurrentHashMap<String, Long>();
        cTable = new ConcurrentHashMap<String, NodeRWLock>();
        cTableLockList = new ArrayList<ReentrantLock>();
        for(int i=0;i<nLocks;i++){
            cTableLockList.add(new ReentrantLock());
        }
    }

    // returns true if there is no conflict, else false
    public synchronized long syncValidate(long startTS, RWSetInfo rwSet,
                                          HashSet<String> conflictSet) {
        return speculativeValidate(startTS, rwSet, conflictSet);
    }

    public long speculativeValidate(long startTS, RWSetInfo rwSet,
                                    HashSet<String> conflictSet) {

//		 System.out.println("Speculative checking readset : " + rwSet.readSet);
//         System.out.println("Speculative checking writeset : " + rwSet.writeSet);

        // check read set
        boolean conflict = false;
        long ret = 0; // 0 means no conflict
        Enumeration keys = conflictTable.keys();

        for (String nodeId : rwSet.readSet) {
            Long writerTS = conflictTable.get(nodeId);
            // if writerTS > startTS => conflict!

            if (writerTS != null && writerTS.longValue() > startTS) {
                conflict = true;
                ret = writerTS.longValue();
                conflictSet.add(nodeId);
            }
        }
        // check write set
        for (String nodeId : rwSet.writeSet) {
            Long writerTS = conflictTable.get(nodeId);
            // if writerTS > startTS => conflict!
            if (writerTS != null && writerTS.longValue() > startTS) {
                conflict = true;
                ret = writerTS.longValue();
                conflictSet.add(nodeId);
            }
        }

        return ret;

    }

    public long validate(long startTS, RWSetInfo rwSet,
                         HashSet<String> conflictSet) {
//         System.out.println("Valdation Read set : " + rwSet.readSet);
//        System.out.println("Validation Write set : " + rwSet.writeSet);
        boolean conflict = false;
        long ret = 0; // 0 means no conflict
        // Acquire locks
        boolean lock = acquireLocksOnWriteSet(rwSet);
        if (lock){
            for (String nodeId : rwSet.readSet) {
                Long writerTS = conflictTable.get(nodeId);
                // if writerTS > startTS => conflict!

                if (writerTS != null && writerTS.longValue() > startTS) {
                    conflict = true;
                    ret = writerTS.longValue();
                    conflictSet.add(nodeId);
                }
            }
            // check write set
            for (String nodeId : rwSet.writeSet) {
                Long writerTS = conflictTable.get(nodeId);
                // if writerTS > startTS => conflict!
                if (writerTS != null && writerTS.longValue() > startTS) {
                    conflict = true;
                    ret = writerTS.longValue();
                    conflictSet.add(nodeId);
                }
            }
        }

        //  September 28, 2015  -  Do not release locks if we have not yet if the transaction will
        // be committing, i.e conflict is false

        if (conflict || !lock ) {
            releaseLock(rwSet);
            ret = 1;//TODO client sees 0 means no conflict
        }

        return ret;
    }

    public boolean acquireLocksOnWriteSet(RWSetInfo rwSet) {
        // System.out.println("Acquiring lock readset:" + rwSet.readSet);
        // System.out.println("Acquring lock writeset: " + rwSet.writeSet);

        TreeSet<String> sortedRWSet = new TreeSet<String>();
        sortedRWSet.addAll(rwSet.readSet);
        sortedRWSet.addAll(rwSet.writeSet);
        boolean bAcquired;
//		System.out.println(sortedRWSet);
        for (String s : sortedRWSet) {
            if (!(s==null) && !s.isEmpty()) {
//				System.out.println("Node is "+s);
                if (cTable.containsKey(s)) {
                    try {
                        LockMode lockMode;
                        if(rwSet.writeSet.contains(s)){
                            lockMode = LockMode.WRITE;
                        }
                        else{
                            lockMode = LockMode.READ;
                        }
//						System.out.println("Trying to acquire lock for "+s+" in "+lockMode);
                        //cTable.get(s).getLock(lockMode);
                        bAcquired = cTable.get(s).tryLock(lockMode);
                        if( !bAcquired ) return false;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                } else {
                    ReentrantLock lock = cTableLockList.get(Math.abs(s.hashCode())%nLocks);
                    lock.lock();
                    if (!cTable.containsKey(s)){
                        cTable.put(s, new NodeRWLock());
                    }
                    lock.unlock();
                    try {
                        LockMode lockMode;
                        if(rwSet.writeSet.contains(s)){
                            lockMode = LockMode.WRITE;
                        }
                        else{
                            lockMode = LockMode.READ;
                        }
                        //cTable.get(s).getLock(lockMode);
                        bAcquired = cTable.get(s).tryLock(lockMode);
                        if( !bAcquired ) return false;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                }
            }
        }
        // System.out.println("Acquired lock readset:" + rwSet.readSet);
        // System.out.println("Acquired lock writeset: " + rwSet.writeSet);

        return true;
    }

    public void releaseLock(RWSetInfo rwSet) {
//		System.out.println("Release rwSet is called!!!!!!!!");
        TreeSet<String> sortedRWSet = new TreeSet<String>();
        sortedRWSet.addAll(rwSet.readSet);
        sortedRWSet.addAll(rwSet.writeSet);
        for (String s : sortedRWSet) {
            if (cTable.containsKey(s)) {
                try {
                    cTable.get(s).releaseLock();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        // System.out.println("Released lock readset:" + rwSet.readSet);
        // System.out.println("Released lock writeset: " + rwSet.writeSet);
    }

    public synchronized void update(long cmtTS, RWSetInfo rwSet) {
        for (String nodeId : rwSet.writeSet) {
            conflictTable.put(nodeId, cmtTS);
        }
    }

    // returns true if no conflict else false
    public long validateAndUpdate(long startTS, long cmtTS,
                                  RWSetInfo rwSet, HashSet<String> refetchSet) {
        long conflictingTS = validate(startTS, rwSet, refetchSet);
//		System.out.println("Finished validation!");
        // no conflict found, update write items
//		System.out.println("Conflicting ts:"+conflictingTS);
        if (conflictingTS == 0) {
            update(cmtTS, rwSet);
            releaseLock(rwSet);   // added here on September 28, 2015
        } else {
            for (String nodeId : rwSet.unusedSet) { // check if any of the item
                // in unused set is modified
                Long writerTS = conflictTable.get(nodeId);
                if (writerTS != null && writerTS.longValue() > startTS) {
                    refetchSet.add(nodeId);
                }
            }
            // System.out.println("Refetch set: "+refetchSet.toString());
        }
        return conflictingTS;
    }

    public void updateAndReleaseLock( long cmtTS, RWSetInfo rwSetInfo){

        //int successStatus = 1;
        //try {
            update(cmtTS, rwSetInfo);
            releaseLock(rwSetInfo);
        //}catch (Exception e){
        //    successStatus = 0;
        //}
        //return successStatus;
    }


    class NodeRWLock {
        boolean writeLock = false;
        int readCount = 0;
        long timeStamp;

        public long getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(long timeStamp) {
            this.timeStamp = timeStamp;
        }


        public synchronized boolean tryLock(LockMode lockMode) {

            boolean acquiredLock = false;
            switch ( lockMode ){
                case READ:  acquiredLock = tryReadLock();
                            break;
                case WRITE: acquiredLock = tryWriteLock();
                            break;
            }
            return acquiredLock;
        }

        public synchronized boolean tryReadLock() {
            if (writeLock) {
                return false;
            } else {
                readCount++;
                return true;
            }
        }

        public synchronized boolean tryWriteLock() {
            if ( (writeLock) || (readCount>0) ) {
                return false;
            } else {
                writeLock = true;
                return true;
            }
        }

        /**
         * wait on this object
         *
         * @throws InterruptedException
         */
        public synchronized void getLock(LockMode lockMode) throws InterruptedException {
            if (lockMode == LockMode.READ){
                while(writeLock){
                    try{
                        //waitForLock.waitForLock(node,lockRequestInfo.transactionId);
                        //System.out.println("Waiting for Read Lock");
                        this.wait();
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                }
                readCount++;
//				System.out.println("Acquired READ lock");
            }
            else{
                while((writeLock) || (readCount>0)){
                    try{
                        //waitForLock.waitForLock(node,lockRequestInfo.transactionId);
                        //System.out.println("Waiting for Write Lock");
                        this.wait();
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                }
                writeLock = true;
//				System.out.println("Acquired WRITE lock");
            }
        }

        /**
         * Release the lock and notify others.
         */
        public synchronized void releaseLock() {
            if(readCount>0 && writeLock==false){
                readCount--;
                if(readCount == 0){
                    notify();
//					System.out.println("Read lock released");
                }
            }
            else if(readCount==0 && writeLock==true){
                writeLock = false;
                notifyAll();
//				System.out.println("Write lock released");
            }
            else if(readCount==0 && writeLock==false){
                // Do Nothing - Releasing the non-acquired lock
            }
            else{ // read count > 0  and write lock is true
                //				System.out.println("Conflicting locks granted!!!!!!!!!!!!! readCount="+lockInfo.readCount+" writeLock="+lockInfo.writeLock);
                try{
                    throw new Exception("Conflicting locks granted!");
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    enum LockMode{
        READ,
        WRITE
    }
}
