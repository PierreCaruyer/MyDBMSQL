package univlille.m1info.abd.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;

public class DefaultIndex implements Index{

	protected String relName;
	protected String attribute;
	protected DefaultRelation rel;
	protected String[] sorts;
	protected int attributeRank = -1;
	protected SchemawithMemory sgbd;
	protected Map<String, List<Integer>> index;

	public DefaultIndex(String relName, String attribute, SchemawithMemory sgbd) {
		this.sgbd = sgbd;
		this.relName = relName;
		this.attribute = attribute;
		this.rel = sgbd.getRelation(relName);
		this.sorts = rel.getRelationSchema().getSort();
		
		for(int i = 0; i < sorts.length && attributeRank < 0; i++)
			if(sorts[i].equals(attribute))
				attributeRank = i;
		
		index = new Hashtable<>();
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
		try{
			Page page = sgbd.getMemoryManager().loadPage(address);
			
			for(String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple()) {
				List<Integer> indexedTuples = getListofAddresses(tuple);
				if(indexedTuples == null)
					indexedTuples = new ArrayList<>();
				indexedTuples.add(address);
				addElement(Arrays.toString(tuple), address);
			}
			
			sgbd.getMemoryManager().releasePage(address, false);
		} catch(NotEnoughMemoryException e) {
			e.printStackTrace();
		}
	}
	
	public void updateKeyValues(String key, List<Integer> values) {
		
	}
	
	public SchemawithMemory getSgbd() {
		return sgbd;
	}
}
