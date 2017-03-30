package beehive.validator;

import beehive.validator.util.Logger;
import beehive.validator.util.RWSetInfo;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by khand055 on 3/23/17.
 */
public class CertifierReplica extends UnicastRemoteObject implements PeerInterface, ValidationService {

    long stableTS;

    //Keeping track of all
    ReplicationConfig replicaConfig;
    TimestampManager timestampManager;
    ExecutorService globalExecutors;
    Validator validator;
    Logger logger;

    long transactionId = 0;

    //Map<Long, ArrayList<RWSetInfo>> pendingValidationRequest = new ConcurrentHashMap<Long, ArrayList<RWSetInfo>>();
    //Transaction Id to RWSet
    Map<Long, RWSetInfo> pendingValidationRequest = new ConcurrentHashMap<Long, RWSetInfo>();

    public long getStableTS() {
        return stableTS;
    }
    public void setStableTS(long stableTS) {
        this.stableTS = stableTS;
    }

    public CertifierReplica(String validationReplicationConfigFile, int serverId, String configFile) throws RemoteException, IOException {

        //Initialization done by all server
        replicaConfig = new ReplicationConfig(validationReplicationConfigFile, serverId, this, configFile);
        globalExecutors = Executors.newCachedThreadPool();

        //Initialization done by Host 0 server
        if( this.replicaConfig.myID == 0){
            timestampManager = new TimestampManager();
        }

        validator = new Validator();
        logger = new Logger();
        logger.setDebug(false);
        logger.setWarning(true);

    }



    public static void main(String[] args) throws Exception {

        int serverId = Integer.parseInt(args[0]);
        String validationReplicationConfigFile = args[1];
        //long numOfNodes = Long.valueOf(args[2]).longValue(); //Number of vertices
        String configFile = args[2];

        CertifierReplica certifierReplica = new CertifierReplica(validationReplicationConfigFile, serverId, configFile);
        certifierReplica.setTransactionId(certifierReplica.replicaConfig.myID);

    }

    @Override
    public long reportCompletion(long commitTS) throws RemoteException {

        long currentSTS;
        if( replicaConfig.myID == 0){ //I am the master
            timestampManager.reportDone(commitTS);
            currentSTS = timestampManager.getSTS();
            return currentSTS;
        } else { // Make request to master
            currentSTS =  replicaConfig.peerHandleTable.get(0).reportCompletion(commitTS);
        }
        return currentSTS;
//        return sendReportCompletion(commitTS);
//        timestampManager.reportDone(commitTS);
//        long currentSTS = timestampManager.getSTS();
//        return currentSTS;
    }

    @Override
    public long reportCompletion(Vector<Long> commitTSList) throws RemoteException {
        return 0;
    }

    @Override
    //Called by Client
    public ValidateResponse validate(long startTS, RWSetInfo rwSet, LoadInfo loadInfo) throws RemoteException {

        long commitTS;

        //Get the STS and CTS from TimestampManager
//        if( this.replicaConfig.myID == 0){
//            sts = timestampManager.getSTS();
//            commitTS = timestampManager.getGTS();
//        } else {
            stableTS = (long)replicaConfig.peerHandleTable.get(0).getSTS();
            commitTS = (long)replicaConfig.peerHandleTable.get(0).getGTS();
//        }

        //Create a coordinator object
        Coordinator coordinator = new Coordinator(startTS, commitTS, rwSet, this, globalExecutors);
        ValidateResponse vr = null;
        try {
            vr = coordinator.validate();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return vr;
    }

    @Override
    public Hashtable<String, ValidateResponse> validateBatch(Queue<ValidationRequest> requestBatch, LoadInfo myLoad) throws RemoteException {
        return null;
    }


    @Override
    //Called by Peer Server
    public PeerResponse validate(long startTS, RWSetInfo rwSet, long transactionId) throws RemoteException {

        PeerResponse res = new PeerResponse();
//        if( !pendingValidationRequest.containsKey(transactionId) ){
//            pendingValidationRequest.put(transactionId, new ArrayList<RWSetInfo>());
//        }
        pendingValidationRequest.put(transactionId, rwSet);
        //logger.logDebug("TID : " + transactionId + " Added TID to PVR ");
        long response = validator.validate(startTS, rwSet, new HashSet<>());
        res.responseType = (response == 0) ? PeerResponse.ALL_LOCKS_ACQUIRED : PeerResponse.ABORT;

        return res;
    }

    @Override
    public long getGTS() throws RemoteException{

        long sts;
        if( replicaConfig.myID == 0){ //I am the master
            sts= timestampManager.getGTS();
        } else { // Make request to master
            sts= replicaConfig.peerHandleTable.get(0).getGTS();
        }
        return sts;
    }

    @Override
    public long getSTS() {
        return timestampManager.getSTS();
    }

    @Override
    public PeerResponse commit(long commitTS, long transactionId) throws RemoteException {

        PeerResponse res = new PeerResponse();
        validator.updateAndReleaseLock(commitTS, pendingValidationRequest.get(transactionId));
        pendingValidationRequest.remove(transactionId);
        //logger.logDebug("TID : " + transactionId + " Remove TID after COMMIT ");
        res.responseType = PeerResponse.COMMITED;
        return res;
        //res.responseType = (response != 0) ? PeerResponse.ALL_LOCKS_ACQUIRED : PeerResponse.ABORT;
    }

    @Override
    public void abort(long transactionId) throws RemoteException {

        validator.releaseLock(pendingValidationRequest.get(transactionId));
        pendingValidationRequest.remove(transactionId);

    }

    public synchronized long getTransactionId(){
        long tid = transactionId;
        setTransactionId(transactionId + replicaConfig.serverList.size());
        return tid;
    }

    public synchronized void setTransactionId(long transactionId){
        this.transactionId = transactionId;
    }
}
