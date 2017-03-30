package beehive.validator;

/**
 * Created by khand055 on 3/23/17.
 */
public class CoordinationRequestStatus {

    public static final int NOT_INITIATED = 0;
    public static final int IN_PHASE_1 = 1;
    public static final int PHASE_1_COMPLETE = 2;
    public static final int IN_PHASE_2 = 3;
    public static final int COMPLETED = 4;

    int cmtVoteCnt;		// num of participants who have voted 'Commit'
    int completedVoteCnt; // num of peer server who completed (either commited or aborted)
    int status;
    int validationOutcome;	// outcome of the phase1 of 2PC request
    int phase1ResponseCount;

}
