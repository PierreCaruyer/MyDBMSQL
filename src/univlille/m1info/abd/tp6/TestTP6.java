package univlille.m1info.abd.tp6;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.phys.JoinOperator;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.phys.ProjectionOperator;
import univlille.m1info.abd.phys.SelectionOperator;
import univlille.m1info.abd.phys.SequentialAccessOnARelationOperator;
import univlille.m1info.abd.phys.SimpleMemoryManager;
import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.schema.DefaultRelationSchema;
import univlille.m1info.abd.schema.RelationSchema;

public class TestTP6 {
	
	public static final int PAGE_SIZE = 20;
	public static final int ATTRIBUTE_SIZE = 20;

	public SequentialAccessOnARelationOperator getRightLoadedTable(MemoryManager mem) {
		RelationSchema schema = new DefaultRelationSchema("RELONE", new String[]{"attrA", "attrB", "attrC"});
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();
		tuples.add(new String[]{"a5", "b1", "c3"});
		tuples.add(new String[]{"a1", "b4", "c6"});
		tuples.add(new String[]{"a2", "b5", "c2"});
		tuples.add(new String[]{"a3", "b8", "c7"});
		relation.loadTuples(tuples);
		SequentialAccessOnARelationOperator tableRelation = new SequentialAccessOnARelationOperator(relation, mem);
		return tableRelation;
	}
	
	public SequentialAccessOnARelationOperator getLeftLoadedTable(MemoryManager mem) {
		RelationSchema schema = new DefaultRelationSchema("RELTWO", new String[]{"attrE", "attrD", "attrA"});
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();
		tuples.add(new String[]{"e4", "d1", "a5"});
		tuples.add(new String[]{"e6", "d4", "a4"});
		tuples.add(new String[]{"e9", "d5", "a3"});
		tuples.add(new String[]{"e6", "d3", "a2"});		
		relation.loadTuples(tuples);
		SequentialAccessOnARelationOperator tableRelation = new SequentialAccessOnARelationOperator(relation, mem);
		return tableRelation;
	}
	
	public SelectionOperator getSelectionOperator(MemoryManager mem) {
		return new SelectionOperator(getRightLoadedTable(mem), "attrA", "a5", ComparisonOperator.EQUAL, mem);
	}
	
	public ProjectionOperator getProjectionOperator(MemoryManager mem) {
		return new ProjectionOperator(getRightLoadedTable(mem), mem, new String[]{"attrA", "attrC"});
	}
	
	public JoinOperator getJoinOperator(MemoryManager mem) {
		return new JoinOperator(getRightLoadedTable(mem), getLeftLoadedTable(mem), mem);
	}
	
	@Test
	public void testCorrectSelectionOperatorWithMemory() {
		System.out.println("Test selection");
		final MemoryManager mem = new SimpleMemoryManager(PAGE_SIZE,ATTRIBUTE_SIZE);
		SelectionOperator selection = getSelectionOperator(mem);
		int page = selection.nextPage();
		try {
			Page p = mem.loadPage(page);
			List<String[]> tupleArray = retrievePageTuples(p);
			
			displayTupleArray(tupleArray);
			
			List<String[]> expectedArray = new ArrayList<>();
			expectedArray.add(new String[]{"a5", "b1", "c3"});
			
			assertTrue(pageContentEquals(expectedArray, tupleArray));
		} catch (NotEnoughMemoryException e) {
			fail();
		}
	}
	
	@Test
	public void testCorrectProjectionOperatorWithMemory() {
		System.out.println("Test projection");
		final MemoryManager mem = new SimpleMemoryManager(PAGE_SIZE,ATTRIBUTE_SIZE);
		ProjectionOperator selection = getProjectionOperator(mem);
		int page = selection.nextPage();
		try {
			Page p = mem.loadPage(page);
			List<String[]> tupleArray = retrievePageTuples(p);

			displayTupleArray(tupleArray);
			
			List<String[]> expectedArray = new ArrayList<>();
			expectedArray.add(new String[]{"a5", "c3"});
			expectedArray.add(new String[]{"a1", "c6"});
			expectedArray.add(new String[]{"a2", "c2"});
			expectedArray.add(new String[]{"a3", "c7"});
			
			assertTrue(pageContentEquals(expectedArray, tupleArray));
		} catch (NotEnoughMemoryException e) {
			fail();
		}
	}
	
	@Test
	public void testCorrectJoinOperatorWithMemory() {
		System.out.println("Test join");
		final MemoryManager mem = new SimpleMemoryManager(PAGE_SIZE,ATTRIBUTE_SIZE);
		JoinOperator join = getJoinOperator(mem);
		int page = join.nextPage();
		try {
			Page p = mem.loadPage(page);
			List<String[]> tupleArray = retrievePageTuples(p);
			
			//displayTupleArray(tupleArray);
			
			List<String[]> expectedArray = new ArrayList<>();
			expectedArray.add(new String[]{"a2", "b5", "c2", "e6", "d3"});
			expectedArray.add(new String[]{"a3", "b8", "c7", "e9", "d5"});
			expectedArray.add(new String[]{"a5", "b1", "c3", "e4", "d1"});
			
			assertTrue(pageContentEquals(expectedArray, tupleArray));
		} catch (NotEnoughMemoryException e) {
			fail();
		}
	}
	public boolean pageContentEquals(List<String[]> expected, List<String[]> actual) {
		if(expected.size() != actual.size())
			return false;
		for(int i = 0; i < expected.size(); i++) {
			String[] tuple = expected.get(i);
			String[] actualTuple = expected.get(i);
			for(int j = 0; j < tuple.length; j++) {
				if(!tuple[j].equals(actualTuple[j]))
					return false;
			}
		}
		return true;
	}
	
	private List<String[]> retrievePageTuples(Page p) {
		List<String[]> tupleArray = new ArrayList<>();
		p.switchToReadMode();
		for(String[] tuple = p.nextTuple(); tuple != null; tuple = p.nextTuple())
			tupleArray.add(tuple);
		return tupleArray;
	}
	
	private void displayTupleArray(List<String[]> tuples) {
		for(String[] tuple : tuples)
			printTuple(tuple);
	}
	
	public static void printTuple(String[] t) {
		System.out.print("[");
		for(String a : t)
			System.out.print(a + ", ");
		System.out.println("]");
	}
}
