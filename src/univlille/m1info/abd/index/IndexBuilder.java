
package univlille.m1info.abd.index;

//Deprecated
public class IndexBuilder {
/*
	public static DefaultIndex build(String relation, String attribute, SchemawithMemory sgbd) {
		try {
			DefaultIndex index = new DefaultIndex(relation, attribute, sgbd);
			DefaultRelation rel = sgbd.getRelation(relation);
			int address = rel.getFirstPageAddress();
			
			do {
				System.out.println(address);
				index.createIndex(address);
				Page page = SchemawithMemory.mem.loadPage(address);
				address = page.getAddressnextPage();
				SchemawithMemory.mem.releasePage(address, false);
			} while (address != -1);

			return index;
		} catch (NotEnoughMemoryException e) {
			return null;
		}
	}*/
}
