package beehive.validator.util;

import java.io.*;
import java.util.HashSet;

public class RWSetInfo implements Externalizable{
	public HashSet<String> readSet;
	public HashSet<String> writeSet;
	public HashSet<String> unusedSet;

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.readSet = (HashSet<String>) in.readObject();
		this.writeSet = (HashSet<String>) in.readObject();
		this.unusedSet = (HashSet<String>) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(this.readSet);
		out.writeObject(this.writeSet);
		out.writeObject(this.unusedSet);
	}
	
	public RWSetInfo()
	{
		this.readSet = new HashSet<String>();
		this.writeSet = new HashSet<String>();
		this.unusedSet = new HashSet<String>();
	}

	public String toString() {
		return "ReadSet:" + readSet.toString() + "\t" + "WriteSet:"+ writeSet.toString() + "\tUnusedSet:"+ unusedSet.toString();
	}
}

