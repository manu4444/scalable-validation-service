package beehive.validator;

import beehive.validator.util.Logger;
import beehive.validator.util.RWSetInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by maste on 3/25/2017.
 */
public class ClientTest extends Thread {

    public static Integer totalAborts = 0;
    public static Integer totalCommits = 0;
    public static int iterationCount;
    public static int nodeCount;
    public static Map<Integer, ValidationService> certifierHandleTable; //HostId to certifier handle mapping
    public long startTS = 0;
    public static Logger logger;
    public int clientId;

    public ClientTest( int clientId){
        this.clientId = clientId;
    }

    @Override
    public void run() {

        //Selecting random server
        List<Integer> keysAsArray = new ArrayList<Integer>(certifierHandleTable.keySet());
        Random r = new Random();


        ValidateResponse validateResponse;

        for( int i=1; i<=iterationCount; i++){

//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            //Randomly getting the server ID
//            if( clientId == 3){
//                try {
//                    Thread.sleep(200);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
            int serverId = keysAsArray.get(r.nextInt(keysAsArray.size()));

            //Randomly get the length of read/write set
            int rwSetLength = r.nextInt(100) + 1; // Random between 1 to 10
            int readLength = r.nextInt(rwSetLength) + 1;
            int writeLength = rwSetLength - readLength;


            //logger.logDebug(" rwSetLength : "+rwSetLength+" readLength : "+readLength+" writeLength "+writeLength);

            RWSetInfo rwSetInfo = new RWSetInfo();

            while( readLength!= 0) {
                int randomeNode = r.nextInt(nodeCount);
                if( rwSetInfo.readSet.contains(Integer.toString(randomeNode))) {
                    continue;
                } else {
                    rwSetInfo.readSet.add(Integer.toString(randomeNode));
                    readLength--;
                }
            }

            while( writeLength!= 0) {
                int randomeNode = r.nextInt(nodeCount);
                if( rwSetInfo.writeSet.contains(Integer.toString(randomeNode))) {
                    continue;
                } else {
                    rwSetInfo.writeSet.add(Integer.toString(randomeNode));
                    writeLength--;
                }
            }
            //logger.logDebug(" rwSetLength : "+rwSetLength+" readLength : "+readLength+" writeLength "+writeLength);

            ValidationService certifierHandle = (ValidationService)certifierHandleTable.get(serverId);
            try {

                logger.logInfo("---------------------------------------------------------------------------");
                logger.logInfo(" Thread ID : "+clientId+" Iteration Count : "+i+" STS "+startTS);
                logger.logInfo(" RWSetLength : "+rwSetLength);
                logger.logInfo(" ReadSet : "+rwSetInfo.readSet.toString());
                logger.logInfo(" WriteSet : "+rwSetInfo.writeSet.toString());

                LoadInfo loadInfo = new LoadInfo(); //Dummy Object for backward compatibility
                validateResponse = certifierHandle.validate(startTS, rwSetInfo, loadInfo);
                logger.logDebug("Response CLient Id: " + clientId + " iteration " + i);
                startTS = validateResponse.latestSTS;


                if( validateResponse.commit ){
                    synchronized (totalCommits){
                        totalCommits++;
                    }

                    //Write Data to the Storage
                    //Thread.sleep(100);
                    logger.logInfo(" Status : COMMIT    CTS : "+validateResponse.commitTS);
                    startTS = certifierHandle.reportCompletion(validateResponse.commitTS);
                } else {
                    synchronized (totalAborts){
                        totalAborts++;
                    }
                    logger.logInfo(" Status : ABORT");
                }
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String args[]) throws Exception{

        logger = new Logger();
        logger.setDebug(true);
        logger.setInfo(true);

        certifierHandleTable = new HashMap<Integer, ValidationService>();
        prepareCertifierHandle(args[0]);
        startTest(Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
    }


    public static void startTest(int threadCount, int itrCount, String configFile) throws InterruptedException{

        long startTimestamp = System.currentTimeMillis();
        iterationCount = itrCount;
        Config configParams = new Config(configFile);
        //configParams = Config.getInstance();
        nodeCount = configParams.getIntValue("GlobalWorkpool.NODE_SIZE");


        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 1; i <= threadCount; i++) {
            executor.execute(new ClientTest(i));
        }

        executor.shutdown();
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.logInfo("Awaiting completion of threads.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


//        for (int i = 1; i <= threadCount; i++) {
//            ClientTest client = new ClientTest(i);
//            client.start();
//            client.join();
//        }

        logger.logInfo("---------------------------------------------------------------------------");
        logger.logInfo("Total COMMIT :" + totalCommits);
        logger.logInfo("Total ABORT :" + totalAborts);
        logger.logInfo("Percentage of ABORT :" + (totalAborts*100)/(totalAborts + totalCommits)+"%");
        logger.logInfo("Total Execution time (millisec) :" + (System.currentTimeMillis() - startTimestamp));

    }


    private static void prepareCertifierHandle(String configFile) throws IOException {

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
            //serverList.add(hostId);
            String url  = "//" + hostname + ":" + Registry.REGISTRY_PORT  + "/ValidationService" + hostId;

            ValidationService certifierHandle = lookup(url);
            certifierHandleTable.put(hostId, certifierHandle);

        }
    }

    public static ValidationService lookup(String url) {
        System.out.println("Looking up certifier:"+url);
        while(true) {
            try {
                ValidationService certifierHandle = (ValidationService) Naming.lookup(url);
                System.out.println("certifier up");
                return certifierHandle;
            } catch(Exception e){
                try { Thread.sleep(10); } catch(Exception e1){}
            }
        }
    }
}
