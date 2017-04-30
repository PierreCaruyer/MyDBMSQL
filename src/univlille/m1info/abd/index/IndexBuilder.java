package univlille.m1info.abd.index;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;

public class IndexBuilder {

	public static DefaultIndex buildIndex(String relation, String attribute, SchemawithMemory sgbd) {
		try {
			DefaultIndex index = new DefaultIndex(relation, attribute, sgbd);
			DefaultRelation rel = sgbd.getRelation(relation);
			int address = rel.getFirstPageAddress();
			Page page = SchemawithMemory.mem.loadPage(address);
			do {
				index.createIndex(address);
				SchemawithMemory.mem.releasePage(address, false);
				if((address = page.getAddressnextPage()) != -1)
					page = SchemawithMemory.mem.loadPage(address);
			} while (address != -1);

			return index;
		} catch (NotEnoughMemoryException e) {
			return null;
		}
	}
	
	public static UpdatableIndex buildTemporaryIndex(DefaultIndex index) {
		return new UpdatableIndex(index.getRelationName(), index.getAttributeName(), index.getSgbd());
	}
}