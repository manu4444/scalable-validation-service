package beehive.validator;

import java.io.Serializable;

/**
 * Created by khand055 on 3/23/17.
 */
public class PeerResponse implements Serializable {

    public static final int ALL_LOCKS_ACQUIRED = 1;	//all locks have been acquired
    public static final int QUEUED = 2;	//at least one lock req is queued
    public static final int ABORT = 4;	//abort
    public static final int COMMITED = 5;	//abort

    public int responseType;
}
