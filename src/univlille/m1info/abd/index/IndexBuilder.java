
package univlille.m1info.abd.index;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;

public class IndexBuilder {

	public static Index build(String relation, String attribute, SchemawithMemory sgbd) {
		try {
			DefaultIndex index = new DefaultIndex(relation, attribute, sgbd);
			DefaultRelation rel = sgbd.getRelation(relation);
			
			for(int address = rel.getFirstPageAddress(); address != -1; ) {
				Page currentPage = SchemawithMemory.mem.loadPage(address);
				int nextPageAddress = currentPage.getAddressnextPage();
				SchemawithMemory.mem.releasePage(address, false);
				address = nextPageAddress;
			}

			return index;
		} catch (NotEnoughMemoryException e) {
			return null;
		}
	}
}
