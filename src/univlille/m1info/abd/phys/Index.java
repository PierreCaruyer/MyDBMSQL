package univlille.m1info.abd.phys;

import java.util.Iterator;

public interface Index {

	// Get the name of the relation to which is related the index
	public String getRelationName ();
	// Get the rank related to the attribute
	public int getRankofAttribute ();
	// Get an iterator of the addresses for a tuple of values
	public Iterator<Integer> getListofAddresses (String[] tuple);
	
	
}
