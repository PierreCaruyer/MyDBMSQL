package univlille.m1info.abd.tp6;

import static org.junit.Assert.assertTrue;

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
	public static final MemoryManager mem = new SimpleMemoryManager(PAGE_SIZE,ATTRIBUTE_SIZE);

	public SequentialAccessOnARelationOperator getRightLoadedTable(MemoryManager mem) {
		RelationSchema schema = new DefaultRelationSchema("RELONE", new String[]{"attrA", "attrB", "attrC"});
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();
		tuples.add(new String[]{"a5", "b1", "c3"});
		tuples.add(new String[]{"a1", "b4", "c6"});
		tuples.add(new String[]{"a2", "b5", "c2"});
		relation.loadTuples(tuples);
		SequentialAccessOnARelationOperator tableRelation = new SequentialAccessOnARelationOperator(relation, mem);
		return tableRelation;
	}
	
	public SequentialAccessOnARelationOperator getLeftLoadedTable(MemoryManager mem) {
		RelationSchema schema = new DefaultRelationSchema("RELTWO", new String[]{"attrE", "attrB", "attrA"});
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();
		tuples.add(new String[]{"e4", "b1", "a5"});
		tuples.add(new String[]{"e6", "b4", "a1"});
		tuples.add(new String[]{"e9", "b5", "a2"});
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
		
		SelectionOperator selection = getSelectionOperator(mem);
		int page = selection.nextPage();
		try {
			Page p = mem.loadPage(page);
			p.switchToReadMode();
			String[] nextTuple = p.nextTuple();
			List<String[]> tupleArray = new ArrayList<>();
			while(nextTuple != null) {
				tupleArray.add(nextTuple);
				nextTuple = p.nextTuple();
			}
			List<String[]> expectedArray = new ArrayList<>();
			expectedArray.add(new String[]{"a5", "b1", "c3"});
			
			assertTrue(pageContentEquals(expectedArray, tupleArray));
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testCorrectProjectionOperatorWithMemory() {
		ProjectionOperator selection = getProjectionOperator(mem);
		int page = selection.nextPage();
		try {
			Page p = mem.loadPage(page);
			p.switchToReadMode();
			String[] nextTuple = p.nextTuple();
			List<String[]> tupleArray = new ArrayList<>();
			while(nextTuple != null) {
				tupleArray.add(nextTuple);
				nextTuple = p.nextTuple();
			}
			List<String[]> expectedArray = new ArrayList<>();
			expectedArray.add(new String[]{"a5", "c3"});
			expectedArray.add(new String[]{"a1", "c6"});
			expectedArray.add(new String[]{"a2", "c2"});
			
			assertTrue(pageContentEquals(expectedArray, tupleArray));
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testCorrectJoinOperatorWithMemory() {
		JoinOperator join = getJoinOperator(mem);
		int page = join.nextPage();
		try {
			Page p = mem.loadPage(page);
			p.switchToReadMode();
			String[] nextTuple = p.nextTuple();
			List<String[]> tupleArray = new ArrayList<>();
			while(nextTuple != null) {
				tupleArray.add(nextTuple);
				printTuple(nextTuple);
				nextTuple = p.nextTuple();
			}
			List<String[]> expectedArray = new ArrayList<>();
			expectedArray.add(new String[]{"a5", "b1", "c3", "e4"});
			expectedArray.add(new String[]{"a1", "b4", "c6","e6"});
			expectedArray.add(new String[]{"a2", "b5", "c2", "e9"});
			
			assertTrue(pageContentEquals(expectedArray, tupleArray));
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
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
	
	public static void printTuple(String[] t) {
		System.out.print("[");
		for(String a : t)
			System.out.print(a + ", ");
		System.out.println("]");
	}
}
