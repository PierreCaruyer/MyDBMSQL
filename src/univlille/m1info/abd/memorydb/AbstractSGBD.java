package univlille.m1info.abd.memorydb;

import java.util.HashMap;
import java.util.Map;

import univlille.m1info.abd.schema.DefaultRelationSchema;
import univlille.m1info.abd.schema.RelationSchema;

public abstract class AbstractSGBD<RT> {
	
	private Map<String, RT> data = new HashMap<>();
	
	/** Retrieves the relation with the given name. */
	public RT getRelation (String name) {
		return data.get(name);
	}
		
	/** Creates a new relation with the given name and sort. */
	public RT createRelation (String name, String ... sort) {
		RelationSchema descr = new DefaultRelationSchema(name, sort);
		RT result = newRelation(descr);
		data.put(name, result);
		return result;
	}
	
	protected abstract RT newRelation (RelationSchema schema);

	protected Map<String, RT> getDataMap () {
		return data;
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
