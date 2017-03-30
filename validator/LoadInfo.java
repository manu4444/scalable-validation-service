package beehive.validator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class LoadInfo implements Externalizable, Comparable {
	public int load;
	public boolean tsMatch;
	public String hostname;
	public LoadInfo(int load, String myHost){
		this.load = load;
		this.hostname = myHost;
	}
	public LoadInfo(){
		this.load = 0;
		this.hostname = new String();
		this.tsMatch = false;
	}

	public int compareTo(Object obj){
		LoadInfo ldobj = (LoadInfo) obj;
		if(this.load>ldobj.load)
			
			return 1;
		else if(this.load<ldobj.load)
			return -1;
		else 
			return 0;
		
	}
	public String toString(){
		return "Host: "+hostname+" Load: "+load;
	}
	
	@Override  
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {  
		this.load = in.readInt();
		this.tsMatch = in.readBoolean();
		this.hostname = (String) in.readObject();
	}  

	@Override  
	public void writeExternal(ObjectOutput out) throws IOException {  
		out.writeInt(this.load);
		out.writeBoolean(this.tsMatch);
		out.writeObject(this.hostname);
	}

}
