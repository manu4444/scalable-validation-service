package beehive.validator;

import java.util.LinkedList;

/**
 * Created by khand055 on 3/23/17.
 */
public class TimestampManager {

    long gts = 0;	//GTS
    long sts = 0;	//STS

    LinkedList<Long> incompleteTS;	// the list of ts whose completion is not yet reported.

    public TimestampManager() {
        incompleteTS = new LinkedList<Long>();
    }

    public synchronized long getGTS(){
        gts = gts+1;
        incompleteTS.add(new Long(gts));
        //System.out.println("issued ts "+timestamp);
        return gts;
    }

    public long getSTS() {
        return sts;
    }

    //
    public synchronized void reportDone(long ts){
        // remove this ts from the list uncmtTS
        incompleteTS.remove(new Long(ts));

        // if the one that is completed is the immediately next to the current STS
        // then avance STS
        if(ts == sts+1){
            sts = ts;
        }

        // see if we can advance STS further if the some of the subsequence transaction
        //  have been completed
        if(incompleteTS.size() > 0 ){
            long first = incompleteTS.getFirst();
            if(first-1 > sts)
                sts = first-1;
        } else { // no more incomplete transaction
            // all transactions for which gts was given are committed
            sts = gts;
        }

        //System.out.println("updated STS to "+sts);
    }
}
