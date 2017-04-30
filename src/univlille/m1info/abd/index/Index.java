package univlille.m1info.abd.index;

import java.util.List;

public interface Index {

	// Get the name of the relation to which is related the index
	public String getRelationName ();
	// Get the rank related to the attribute
	public int getRankofAttribute ();
	//
	public String getAttributeName ();
	// Get an iterator of the addresses for a tuple of values
	public List<Integer> getListofAddresses (String[] tuple);
	// Add an address for the key 
	public boolean addElement(String key,int Address);
	
}