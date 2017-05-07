package univlille.m1info.abd.tp6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.phys.PhysicalOperator;

public class TP6 {
	
	private SchemawithMemory sgbd;
	private MemoryManager mem;
	
	public TP6() {
		sgbd = new SchemawithMemory();
		mem = sgbd.getMemoryManager();
	}
	
	public List<String[]> getOperatorTuples(PhysicalOperator operator) throws NotEnoughMemoryException{
		int pageAddress = operator.nextPage();
		List<String[]> tuples = new ArrayList<>();
		while(pageAddress != -1) {
			Page p = mem.loadPage(pageAddress);
			List<String[]> retrievedTuples = retrievePageTuples(p);
			
			for(String[] t : retrievedTuples)
				tuples.add(t);
			
			pageAddress = operator.nextPage();
		}
		return tuples;
	}
	
	private List<String[]> retrievePageTuples(Page p) {
		List<String[]> tupleArray = new ArrayList<>();
		p.switchToReadMode();
		for(String[] tuple = p.nextTuple(); tuple != null; tuple = p.nextTuple())
			tupleArray.add(tuple);
		return tupleArray;
	}
	
	public void displayPageContent(List<String[]> tuples) {
		for(String[] tuple : tuples)
			Arrays.toString(tuple);
	}
	
	public SchemawithMemory getSgbd() {
		return sgbd;
	}
	
	public MemoryManager getMemoryManager() {
		return mem;
	}
}
