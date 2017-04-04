package univlille.m1info.abd.tp6;

import java.util.ArrayList;
import java.util.List;

import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.phys.PhysicalOperator;
import univlille.m1info.abd.phys.SimpleMemoryManager;

public class TP6 {
	private MemoryManager mem;
	
	public TP6(int pageSize, int attrSize) {
		mem = new SimpleMemoryManager(pageSize, attrSize);
	}
	
	public List<String[]> getOperatorTuples(PhysicalOperator operator) throws NotEnoughMemoryException{
		int pageAddress = operator.nextPage();
		List<String[]> tuples = new ArrayList<>();
		while(pageAddress > -1) {
			Page p = mem.loadPage(pageAddress);
			
			List<String[]> retrievedTuples = retrievePageTuples(p);
			displayTupleArray(retrievedTuples);
			
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
	
	public static void displayTupleArray(List<String[]> tuples) {
		for(String[] tuple : tuples)
			printTuple(tuple);
	}
	
	public static void printTuple(String[] t) {
		System.out.print("[");
		for(String a : t)
			System.out.print(a + ", ");
		System.out.println("]");
	}
	
	public MemoryManager getMemoryManager() {
		return mem;
	}
}
