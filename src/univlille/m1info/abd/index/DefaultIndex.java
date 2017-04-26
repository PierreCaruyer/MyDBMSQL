package univlille.m1info.abd.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;

public class DefaultIndex implements Index{

	private String relName;
	private DefaultRelation rel;
	private String[] sorts;
	private int attributeRank = -1;
	private List<Integer> addresses = null;

	public DefaultIndex(String relName, String attribute, SchemawithMemory sgbd) {
		this.relName = relName;
		this.rel = sgbd.getRelation(relName);
		this.sorts = rel.getRelationSchema().getSort();
		addresses = new ArrayList<>();

		for(int i = 0; i < sorts.length && attributeRank < 0; i++)
			if(sorts[i].equals(attribute))
				attributeRank = i;
	}

	public void clear() {
		addresses.clear();
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
		try {
			Page page = null;
			boolean tupleMatch = false;
			for(int address = rel.getFirstPageAddress(); address != -1; address = page.getAddressnextPage()) {
				page = SchemawithMemory.mem.loadPage(address);
				page.switchToReadMode();
				for(String[] pageTuple = page.nextTuple(); pageTuple != null && !tupleMatch; pageTuple = page.nextTuple()) {
					if(tupleEquals(tuple, pageTuple)) {
						addresses.add(address);
						tupleMatch = true;
					}
				}
				tupleMatch = false;
				SchemawithMemory.mem.releasePage(address, false);
			}
			return addresses.iterator();
		} catch(NotEnoughMemoryException e) {
			clear();
			return null;
		}
	}
	
	private boolean tupleEquals(String[] tuple1, String[] tuple2) {
		boolean found = false;
		for(int i = 0; i < tuple1.length; i++) {
			found = false;
			for(int j = 0; j < tuple2.length && !found; j++)
				if(tuple1[i].equals(tuple2[j]))
					found = true;
			if(!found)
				return false;
		}
		return true;
	}
}
