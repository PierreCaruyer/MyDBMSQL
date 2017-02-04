package univlille.m1info.abd.tp2;


import java.util.HashMap;
import java.util.Map;

import univlille.m1info.abd.schema.DefaultRelationSchema;


/** Simulation of a very simple SGBD, in which all the data is in memory.
 * The relations are accessed sequentially.
 * This implementation does not allow concurrent access to the same relation.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 26 janv. 2017
 */
public class SimpleSGBD  {
	
	private Map<String, SimpleDBRelation> data = new HashMap<>();
	
	public SimpleDBRelation createRelation (String name, String ... sort) {
		DefaultRelationSchema descr = new DefaultRelationSchema(name, sort);
		SimpleDBRelation result = new SimpleDBRelation(descr);
		data.put(name, result);
		return result;
	}
	
	public SimpleDBRelation getRelation (String name) {
		return data.get(name);
	}
	
	public void addRelation(String relName, SimpleDBRelation rel){
		data.put(relName, rel);
	}
	
	/** Returns a name for a new relation that is not used in this database.
	 * 
	 * @return
	 */
	public String getFreshRelationName () {
		return TMP_REL_PREFIX + getNextFreshSuffixe();
	}


	// -------------------------------------------------
	// Utility methods for creating fresh relation names
	// -------------------------------------------------
	
	private int tmpRelationsCounter = 0;
	private final String TMP_REL_PREFIX = "Tmp";

	
	protected String getNextFreshSuffixe () {
		if (tmpRelationsCounter >= 26*26)
			throw new UnsupportedOperationException("No more fresh names");
		
		char[] suffixe = new char[2];
		suffixe[0] = (char) ('A' + (tmpRelationsCounter / 26));
		suffixe[1] = (char) ('A' + (tmpRelationsCounter % 26));
		tmpRelationsCounter++;
		return new String(suffixe);
	}
	
}
