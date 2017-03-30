package beehive.validator;

import beehive.validator.util.RWSetInfo;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;

/**
 * Created by khand055 on 3/23/17.
 */
public class Coordinator {

    ReplicationConfig replicaConfig;
    CertifierReplica certifierReplica;
    ExecutorService globalExecutors;
    long startTS;
    long commitTS;
    RWSetInfo rwSetInfo;
    CoordinationRequestStatus crqStatus;
    HashMap<Integer, RWSetInfo> partitionedRWSetMap;
    long transactionId;

//    public static final int NOT_INITIATED = 0;
//    public static final int IN_PHASE_1 = 1;
//    public static final int PHASE_1_COMPLETE = 2;
//    public static final int IN_PHASE_2 = 3;
//    public static final int COMPLETED = 4;
//
//    int cmtVoteCnt;		// num of participants who have voted 'Commit'
//    int status;
//    int validationOutcome;	// outcome of the phase1 of 2PC request

    public Coordinator(long startTS, long commitTS, RWSetInfo rwSetInfo, CertifierReplica certifierReplica, ExecutorService globalExecutors){

        this.certifierReplica = certifierReplica;
        this.replicaConfig = certifierReplica.replicaConfig;
        this.globalExecutors = globalExecutors;
        this.rwSetInfo = rwSetInfo;
        this.startTS = startTS;
        this.commitTS = commitTS;
        crqStatus = new CoordinationRequestStatus();
        crqStatus.status = CoordinationRequestStatus.NOT_INITIATED;
        partitionedRWSetMap = new HashMap<Integer, RWSetInfo>();

        transactionId = certifierReplica.getTransactionId();

        populatePartitionedRWSetMap();
    }

    public void sendReportCompletionTimeManager() throws RemoteException {
        long latestSTS = certifierReplica.reportCompletion(commitTS);
        certifierReplica.setStableTS(latestSTS);
    }

    public void sendAbortStatusToClient(){

    }

    public ValidateResponse validate() throws InterruptedException, RemoteException {


        ValidateResponse vr = new ValidateResponse();
        sendValidationRequestToPeer(); //Blocking Call


        switch ( crqStatus.validationOutcome ){

            case PeerResponse.ABORT :

                sendReportCompletionTimeManager();
                sendAbortRequestToPeer(); //Non-Blocking call
                vr.latestSTS = certifierReplica.getStableTS();
                vr.commit = false;
                break;

            case PeerResponse.ALL_LOCKS_ACQUIRED :

                sendCommitRequestToPeer(); //Non-Blocking Call
                vr.latestSTS = certifierReplica.getStableTS();
                vr.commitTS = commitTS;
                vr.commit = true; //Zero value read by client as COMMIT
                break;
        }

        return vr;
    }

    private void sendAbortRequestToPeer() throws InterruptedException{

        crqStatus.status = CoordinationRequestStatus.IN_PHASE_2;

        for (Integer server : partitionedRWSetMap.keySet()) {
            PeerInterface peerHandle = replicaConfig.peerHandleTable.get(server);
            globalExecutors.submit(new Runnable() {
                public void run() {
                    try {
                        peerHandle.abort(transactionId);
                        //processPeerValidationResponse(response);
                    } catch(Exception e){
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    private void sendCommitRequestToPeer() throws InterruptedException{

        crqStatus.status = CoordinationRequestStatus.IN_PHASE_2;
        for (Integer server : partitionedRWSetMap.keySet()) {
            PeerInterface peerHandle = replicaConfig.peerHandleTable.get(server);
            globalExecutors.submit(new Runnable() {
                public void run() {
                    try {
                        PeerResponse response = peerHandle.commit(commitTS, transactionId);
                        processPeerCommitResponse(response);
                    } catch(Exception e){
                        e.printStackTrace();
                    }
                }
            });

        }
        //waitForStatus(CoordinationRequestStatus.COMPLETED);
    }

    private void processPeerCommitResponse(PeerResponse response){

        synchronized (crqStatus) {

            switch (response.responseType){

                case PeerResponse.COMMITED:
                    crqStatus.completedVoteCnt ++;
                    if( crqStatus.completedVoteCnt == partitionedRWSetMap.keySet().size() ){
                        crqStatus.status = CoordinationRequestStatus.COMPLETED;
//                        notify();
                    }
                    break;
            }

        }
    }

    private void sendValidationRequestToPeer() throws InterruptedException{
        crqStatus.status = CoordinationRequestStatus.IN_PHASE_1;
        crqStatus.cmtVoteCnt = 0;

        for (Integer server : partitionedRWSetMap.keySet()) {

            PeerInterface peerHandle = replicaConfig.peerHandleTable.get(server);

            globalExecutors.submit(new Runnable() {
                public void run() {
                    try {
                        RWSetInfo rwSetInfo = partitionedRWSetMap.get(server);
                        PeerResponse response = peerHandle.validate(startTS, rwSetInfo, transactionId);
                        processPeerValidationResponse(response);
                    } catch(Exception e){
                        e.printStackTrace();
                    }
                }
            });


        }
        waitForStatus(CoordinationRequestStatus.PHASE_1_COMPLETE);
    }

    private void processPeerValidationResponse(PeerResponse response){

        // proceed to phase 2 either if you get all votes/locks or if you get any abort








        synchronized (crqStatus) {

            //This case can never happen as coordinator wait for all phase 1 response

//            if (crqStatus.status > CoordinationRequestStatus.IN_PHASE_1) {
//                // we are getting a delayed response, we have already moved to phase2: Abort
//                // Note: commit can't happen without getting response from all
//                return;
//            }

            if( response.responseType == PeerResponse.ABORT ){
                crqStatus.validationOutcome = PeerResponse.ABORT;
            } else if( response.responseType == PeerResponse.ALL_LOCKS_ACQUIRED ){
                if( crqStatus.validationOutcome != PeerResponse.ABORT ){
                    crqStatus.validationOutcome = PeerResponse.ALL_LOCKS_ACQUIRED;
                }
            }
            crqStatus.phase1ResponseCount++;
            if( crqStatus.phase1ResponseCount == partitionedRWSetMap.keySet().size()) {
                crqStatus.status = CoordinationRequestStatus.PHASE_1_COMPLETE;
                crqStatus.notify();
            }


            //race condition: CoordinationRequestStatus.PHASE_1_COMPLETE might be assigned by any thread and immediately waaitFor this status called
            /*if( crqStatus.status != CoordinationRequestStatus.PHASE_1_COMPLETE){
                switch( response.responseType ){
                    case PeerResponse.ABORT :
                        crqStatus.validationOutcome = PeerResponse.ABORT;
                        crqStatus.status = CoordinationRequestStatus.PHASE_1_COMPLETE;
                        break;

                    case PeerResponse.ALL_LOCKS_ACQUIRED :
                            crqStatus.validationOutcome = PeerResponse.ALL_LOCKS_ACQUIRED;
                            break;
                }
            }
            crqStatus.phase1ResponseCount++;
            if( crqStatus.phase1ResponseCount == partitionedRWSetMap.keySet().size()) {
                crqStatus.status = CoordinationRequestStatus.PHASE_1_COMPLETE;
                certifierReplica.logger.logDebug("TID : " + transactionId + " CRQ Phase1RC :" + crqStatus.phase1ResponseCount + " NOTIFY ");
                crqStatus.notify();
            }*/




            /*switch( response.responseType ){

                case PeerResponse.ABORT :
                    crqStatus.validationOutcome = PeerResponse.ABORT;
                    crqStatus.status = CoordinationRequestStatus.PHASE_1_COMPLETE;
                    crqStatus.notify();
                    break;

                case PeerResponse.ALL_LOCKS_ACQUIRED :
                    crqStatus.cmtVoteCnt++;
                    if (crqStatus.cmtVoteCnt == partitionedRWSetMap.keySet().size()) {
                        crqStatus.validationOutcome = PeerResponse.ALL_LOCKS_ACQUIRED;
                        crqStatus.status = CoordinationRequestStatus.PHASE_1_COMPLETE;
                        crqStatus.notify();
                        break;
                    }
            }
            */
        }
    }

    public void waitForStatus(int status) throws InterruptedException {
        synchronized (crqStatus) {
            while (crqStatus.status != status) {
                crqStatus.wait(60000);
                //certifierReplica.logger.logDebug("TID : " + transactionId + " CRQ Phase1RC :" + crqStatus.phase1ResponseCount);
                if (crqStatus.status != status) {
                    // System.out.println("T"+tid+" timedout in waiting for status "+status+" crqstatus:"+crqStatus.status);
                    // System.out.println(crqStatus.reqStatusMap);
                    System.exit(0);
                    break;
                }


            }
        }
    }

    private void populatePartitionedRWSetMap(){

        int serverId;
        for( String nodeId : rwSetInfo.readSet ){
            serverId = replicaConfig.getServerFromNodeKey(Integer.parseInt(nodeId));
            if( serverId >= 0) {
                if (!partitionedRWSetMap.containsKey(serverId)) {
                    partitionedRWSetMap.put(serverId, new RWSetInfo());
                }
                partitionedRWSetMap.get(serverId).readSet.add(nodeId);
            }
        }

        for( String nodeId : rwSetInfo.writeSet ){
            serverId = replicaConfig.getServerFromNodeKey(Integer.parseInt(nodeId));
            if( serverId >= 0) {
                if (!partitionedRWSetMap.containsKey(serverId)) {
                    partitionedRWSetMap.put(serverId, new RWSetInfo());
                }
                partitionedRWSetMap.get(serverId).writeSet.add(nodeId);
            }
        }
    }
}
