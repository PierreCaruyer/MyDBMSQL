package univlille.m1info.abd.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;

public class DefaultIndex implements Index{

	private String relName;
	private String attribute;
	private DefaultRelation rel;
	private String[] sorts;
	private int attributeRank = -1;
	private Map<String, List<Integer>> index;

	public DefaultIndex(String relName, String attribute, SchemawithMemory sgbd) {
		this.relName = relName;
		this.attribute = attribute;
		this.rel = sgbd.getRelation(relName);
		this.sorts = rel.getRelationSchema().getSort();
		index = new Hashtable<>();
		for(int i = 0; i < sorts.length && attributeRank < 0; i++)
			if(sorts[i].equals(attribute))
				attributeRank = i;
	}

	@Override
	public String getRelationName() {
		return relName;
	}
	
	@Override
	public String getAttributeName() {
		return attribute;
	}

	@Override
	public int getRankofAttribute() {
		return attributeRank;
	}

	@Override
	public List<Integer> getListofAddresses(String[] tuple) {
		return  index.get(Arrays.toString(tuple));
	}
	
	@Override
	public boolean addElement(String key, int address) {
		if (index.containsKey(key)) {
			return (index.get(key)).add(new Integer(address));
		} else {
			List <Integer> tmp= new ArrayList<>();
			tmp.add(new Integer(address));
			index.put(key, tmp);
			return true;
		}
	}
	
	public void createIndex(int address){
		
	}
	
}
