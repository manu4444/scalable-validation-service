package beehive.validator;

import beehive.util.RWSetInfo;

import java.io.Serializable;

public class ValidationRequest implements Serializable {
	public String tid;
	public RWSetInfo rwSet;
	public long startTS;	

	public ValidationRequest(String tid, RWSetInfo rwSet, long startTS) {
		this.tid = tid;
		this.rwSet = rwSet;
		this.startTS = startTS;	
	}
}
