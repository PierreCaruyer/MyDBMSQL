package univlille.m1info.abd.index;

import java.util.Iterator;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;

public class DefaultIndex implements Index{

	private String relName;
	private DefaultRelation rel;
	private String[] sorts;
	private int attributeRank = -1;
	
	public DefaultIndex(String relName, SchemawithMemory sgbd, String attribute) {
		this.relName = relName;
		this.rel = sgbd.getRelation(relName);
		this.sorts = rel.getRelationSchema().getSort();
		
		for(int i = 0; i < sorts.length && attributeRank < 0; i++)
			if(sorts[i].equals(attribute))
				attributeRank = i;
	}

	@Override
	public String getRelationName() {
		return relName;
	}

	@Override
	public int getRankofAttribute() {
		return attributeRank;
	}

	@Override
	public Iterator<Integer> getListofAddresses(String[] tuple) {
		return null;
	}
}
