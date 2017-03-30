package beehive.validator;

import beehive.validator.util.RWSetInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by khand055 on 3/23/17.
 */
public interface PeerInterface extends Remote {

    //Called by peer server to Host 0 (Timestamp Manager running at Host 0)
    public long reportCompletion(long commitTS) throws RemoteException;


    //Calls received from the coordinator
    public PeerResponse validate(long startTS, RWSetInfo rwSet, long transactionId) throws RemoteException;

    //Called by peer server to Host 0 (Timestamp Manager running at Host 0) to get commit timestamp
    public long getGTS() throws RemoteException;

    //Called by peer server to Host 0 (Timestamp Manager running at Host 0) to get stable timestamp
    public long getSTS() throws RemoteException;

    //Sent to each peer to release the lock and commit
    PeerResponse commit(long commitTS, long transactionId) throws RemoteException;

    //Sent to each peer to release the lock
    void abort(long transactionId) throws RemoteException;
}
