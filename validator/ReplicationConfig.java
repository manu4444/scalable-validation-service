package beehive.validator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.util.*;

/**
 * Created by khand055 on 3/23/17.
 */
public class ReplicationConfig {

    Map<Integer, PeerInterface> peerHandleTable; //HostId to peer handle mapping
    //HashMap<Integer, String> indexTable; //Server Id to hostname mapping
    String myHostName;
    int myID;
    long numOfNodes;
    List<Integer> serverList = new ArrayList<Integer>();
    Map<Integer, ArrayList<Long>> keyRangeServerMap  = new HashMap<Integer, ArrayList<Long>>();


    public ReplicationConfig(String validationReplicationConfigFile, int serverId, CertifierReplica localHandle, String configFile)
            throws RemoteException, IOException{

        this.myID = serverId;
        this.peerHandleTable = new HashMap<Integer, PeerInterface>();
        //this.numOfNodes = numOfNode;
        Config configParams = new Config(configFile);
        //configParams = Config.getInstance();
        this.numOfNodes = configParams.getIntValue("GlobalWorkpool.NODE_SIZE");

        selfRegistration(localHandle);
        preparePeerHandle(validationReplicationConfigFile, serverId, localHandle);
        setKeyRangeServerMap(this.numOfNodes);

        for( int key: keyRangeServerMap.keySet()){
            System.out.println(" Server Id: " + key + " Range: " + keyRangeServerMap.get(key).toString());
        }

    }

    public int getServerFromNodeKey(long nodeId){

        List<Long> keyRange;
        for( int serverId : keyRangeServerMap.keySet() ){
            keyRange = keyRangeServerMap.get(serverId);
            if( nodeId >= keyRange.get(0) && nodeId <= keyRange.get(1) ){
                return serverId;
            }
        }
        return -1;
    }

    private void selfRegistration(CertifierReplica localHandle) throws RemoteException{

        //First get the handle to the local registry and bind yourself
        System.setSecurityManager(new RMISecurityManager());
        Registry localRegistry;
        try {
            LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        } catch (ExportException e){
            //registry already running and do nothing
        }
        localRegistry = LocateRegistry.getRegistry(Registry.REGISTRY_PORT);

        localRegistry.rebind("PeerInterface"+myID, (PeerInterface)localHandle);
        localRegistry.rebind("ValidationService"+myID, (ValidationService)localHandle);
        if( myID == 0){ //I am the master
            localRegistry.rebind("WorkpoolServer", (ValidationService)localHandle); // Testing with beehive
        }

    }

    private void preparePeerHandle(String configFile, int serverId, CertifierReplica localHandle) throws IOException{

        BufferedReader bufferedReader = new BufferedReader(new FileReader(configFile));
        String line;
        String hostname;
        int hostId;
        //int i=0;
        while ((line = bufferedReader.readLine()) != null) {
            if( line.charAt(0) == '#') continue;
            String[] splited = line.split("\\s+");
            hostname = splited[0];
            hostId = Integer.parseInt(splited[1]);
            serverList.add(hostId);
            String url  = "//" + hostname + ":" + Registry.REGISTRY_PORT  + "/PeerInterface" + hostId;
            //indexTable.put(i, host);
            //String fullHostName = InetAddress.getLocalHost().getCanonicalHostName();
            if (myID == hostId) {
                myHostName = hostname;
                peerHandleTable.put(hostId, localHandle);
            } else {
                PeerInterface peerHandle = lookup(url);
                peerHandleTable.put(hostId, peerHandle);
            }

        }

    }

    private PeerInterface lookup(String url) {
        System.out.println("Looking up peer:"+url);
        while(true) {
            try {
                PeerInterface peerHandle = (PeerInterface) Naming.lookup(url);
                System.out.println("Peer up");
                return peerHandle;
            } catch(Exception e){
                try { Thread.sleep(10); } catch(Exception e1){}
            }
        }
    }

    private void setKeyRangeServerMap(long numOfNodes){

        int totalClusterSize = peerHandleTable.size();
        long nodePerCluster = numOfNodes/totalClusterSize;
        long residue = numOfNodes%totalClusterSize; // shall be added to last Host

        long start = 0;
        long startIndex = 0;
        long endIndex = 0;

        //Initialize range to -1 fpr all
        for( int i=0; i<serverList.size(); i++ ){
            keyRangeServerMap.put(serverList.get(i), new ArrayList<Long>(Arrays.asList(-1L,-1L)));
        }

        if( nodePerCluster > 0){ //atleast one node per cluster
            for( int i=0; i<serverList.size(); i++ ){
                startIndex = start;
                endIndex = start + nodePerCluster - 1;
                if( i == serverList.size()-1 ) {// last item
                    endIndex += residue;
                }
                keyRangeServerMap.put(serverList.get(i), new ArrayList<Long>(Arrays.asList(startIndex, endIndex)));
                start += nodePerCluster;
            }
        } else if (residue > 0) {
            keyRangeServerMap.put(serverList.get(0), new ArrayList<Long>(Arrays.asList(startIndex, endIndex))); //Put residue in first node
        }
    }
}
