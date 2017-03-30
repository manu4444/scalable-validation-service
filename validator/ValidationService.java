package beehive.validator;

import beehive.validator.util.RWSetInfo;
import beehive.validator.LoadInfo;
import beehive.validator.ValidationRequest;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.Queue;
import java.util.Vector;

/**
 * Created by khand055 on 3/23/17.
 */

//TODO rename the class to ValidationService as in Beehive
public interface ValidationService extends Remote {

    //Calls received from the client
    public ValidateResponse validate(long startTS, RWSetInfo rwSet, LoadInfo myLoad) throws RemoteException;
    public Hashtable<String, ValidateResponse> validateBatch(Queue<ValidationRequest> requestBatch, LoadInfo myLoad) throws RemoteException;

    //Called by client to report the completion of transaction
    public long reportCompletion(long commitTS) throws RemoteException;
    public long reportCompletion(Vector<Long> commitTSList) throws RemoteException;

    public long getSTS() throws RemoteException;

}
