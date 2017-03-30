package beehive.validator;

import beehive.workpool.LoadInfo;

import java.io.*;
import java.util.HashSet;
import java.util.Vector;

/**
 * Created by maste on 3/25/2017.
 */
public class ValidateResponse implements Externalizable {
    public boolean commit;	// commit or abort status
    public long commitTS;	// commit ts if the txn committed
    public long latestSTS;	// latest value of sts
    public HashSet<String> refetchSet; 	// objects in the read-write-set which got modified
    public boolean localResponse=false;	// is the response from local validator
    public boolean speculativeAbort = false;	//in case of abort is the abort speculative
    public Vector<LoadInfo> loadVector;
    public long conflictingTS; //largest conflicting time stamp with which transaction conflicted

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.commit = in.readBoolean();
        this.commitTS = in.readLong();
        this.latestSTS = in.readLong();
        this.refetchSet = (HashSet<String>) in.readObject();
        this.localResponse = in.readBoolean();
        this.speculativeAbort = in.readBoolean();
        this.loadVector = (Vector<LoadInfo>) in.readObject();
        this.conflictingTS = in.readLong();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(this.commit);
        out.writeLong(this.commitTS);
        out.writeLong(this.latestSTS);
        out.writeObject(this.refetchSet);
        out.writeBoolean(this.localResponse);
        out.writeBoolean(this.speculativeAbort);
        out.writeObject(this.loadVector);
        out.writeLong(this.conflictingTS);
    }

    public ValidateResponse(){
        this.commit = false;
        this.commitTS = 0L;
        this.latestSTS = 0L;
        this.refetchSet = new HashSet<String>();
        this.localResponse = false;
        this.speculativeAbort = false;
        this.loadVector = new Vector<LoadInfo>();
        this.conflictingTS = 0L;
    }

}
