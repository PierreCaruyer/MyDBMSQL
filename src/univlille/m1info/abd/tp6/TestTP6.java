package univlille.m1info.abd.tp6;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.phys.JoinOperator;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.PhysicalOperator;
import univlille.m1info.abd.phys.ProjectionOperator;
import univlille.m1info.abd.phys.SelectionOperator;
import univlille.m1info.abd.phys.SequentialAccessOnARelationOperator;
import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.schema.DefaultRelationSchema;
import univlille.m1info.abd.schema.RelationSchema;

public class TestTP6 {

	private static final int PAGE_SIZE = 20;
	private static final int ATTRIBUTE_SIZE = 20;
	private static final int REPEAT = 15;
	private TP6 tp6;
	
	/**
	 * Loads a Short table
	 */
	public SequentialAccessOnARelationOperator getRightLoadedTable(MemoryManager mem) {
		RelationSchema schema = new DefaultRelationSchema("RELONE", new String[]{"attrA", "attrB", "attrC"});
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();
		
		tuples.add(new String[]{"a5", "b1", "c3"});
		tuples.add(new String[]{"a1", "b4", "c6"});
		tuples.add(new String[]{"a2", "b5", "c2"});
		tuples.add(new String[]{"a3", "b8", "c7"});
		
		relation.loadTuples(tuples);
		
		return new SequentialAccessOnARelationOperator(relation, mem); 
	}
	
	/**
	 * Loads a Short table
	 */
	public SequentialAccessOnARelationOperator getLeftLoadedTable(MemoryManager mem) {
		RelationSchema schema = new DefaultRelationSchema("RELTWO", new String[]{"attrE", "attrD", "attrA"});
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();
		
		tuples.add(new String[]{"e4", "d1", "a5"});
		tuples.add(new String[]{"e6", "d4", "a4"});
		tuples.add(new String[]{"e9", "d5", "a3"});
		tuples.add(new String[]{"e6", "d3", "a2"});
		
		relation.loadTuples(tuples);
		
		return new SequentialAccessOnARelationOperator(relation, mem);
	}
	
	/**
	 * Loads a table w/ many tuples to tests memory allocation and free mecanisms' correctness
	 */
	public SequentialAccessOnARelationOperator getLongLoadedTable(MemoryManager mem) {
		RelationSchema schema = new DefaultRelationSchema("RELLONG", new String[]{"attrA", "attrB", "attrC"});
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();
		
		for(int i = 0; i < REPEAT; i++) {
			tuples.add(new String[]{"a5", "b1", "c3"});
			tuples.add(new String[]{"a1", "b4", "c6"});
			tuples.add(new String[]{"a2", "b5", "c2"});
			tuples.add(new String[]{"a3", "b8", "c7"});
		}
		
		relation.loadTuples(tuples);
		
		return new SequentialAccessOnARelationOperator(relation, mem);
	}
	
	//Selection operator w/ few tuples
	public SelectionOperator getShortSelectionOperator(MemoryManager mem) {
		return new SelectionOperator(getRightLoadedTable(mem), "attrA", "a5", ComparisonOperator.EQUAL, mem);
	}
	
	//Selection operator w/ more tuples
	public SelectionOperator getLongSelectionOperator(MemoryManager mem) {
		return new SelectionOperator(getLongLoadedTable(mem), "attrA", "a5", ComparisonOperator.EQUAL, mem);
	}
	
	//Projection operator w/ few tuples
	public ProjectionOperator getShortProjectionOperator(MemoryManager mem) {
		return new ProjectionOperator(getRightLoadedTable(mem), mem, new String[]{"attrA", "attrC"});
	}
	
	//Projection operator w/ more tuples
	public ProjectionOperator getLongProjectionOperator(MemoryManager mem) {
		return new ProjectionOperator(getLongLoadedTable(mem), mem, new String[]{"attrA", "attrC"});
	}
	
	public JoinOperator getJoinOperator(MemoryManager mem) {
		return new JoinOperator(getRightLoadedTable(mem), getLeftLoadedTable(mem), mem);
	}
	
	@Before
	public void setUp() {
		tp6 = new TP6(PAGE_SIZE, ATTRIBUTE_SIZE);
	}
	
	private void synthesizeTest(String testName, PhysicalOperator testOperator, List<String[]> expectedTuples) {
		System.out.println(testName);
		List<String[]> tupleArray;
		try {
			tupleArray = tp6.getOperatorTuples(testOperator);
			System.out.println(tupleArray.size());
			//TP6.displayTupleArray(tupleArray);
			assertTrue(pageContentEquals(expectedTuples, tupleArray));
		} catch (NotEnoughMemoryException e) {
			fail();
		}
	}
	
	@Test
	public void testCorrectShortSelectionOperatorWithMemory() {
		PhysicalOperator selection = getShortSelectionOperator(tp6.getMemoryManager());
		List<String[]> expectedArray = new ArrayList<>();
		expectedArray.add(new String[]{"a5", "b1", "c3"});
		synthesizeTest("Test short selection", selection, expectedArray);
	}
	
	@Test
	public void testCorrectLongSelectionOperatorWithMemory() {
		PhysicalOperator selection = getLongSelectionOperator(tp6.getMemoryManager());
		List<String[]> expectedArray = new ArrayList<>();
		for(int i = 0; i < REPEAT; i++) 
			expectedArray.add(new String[]{"a5", "b1", "c3"});
		synthesizeTest("Test long selection", selection, expectedArray);
	}
	
	@Test
	public void testCorrectShortProjectionOperatorWithMemory() {
		PhysicalOperator projection = getShortProjectionOperator(tp6.getMemoryManager());
		List<String[]> expectedArray = new ArrayList<>();
		
		expectedArray.add(new String[]{"a5", "c3"});
		expectedArray.add(new String[]{"a1", "c6"});
		expectedArray.add(new String[]{"a2", "c2"});
		expectedArray.add(new String[]{"a3", "c7"});
		
		synthesizeTest("Test short projection", projection, expectedArray);
	}
	
	@Test
	public void testCorrectLongProjectionOperatorWithMemory() {
		PhysicalOperator projection = getLongProjectionOperator(tp6.getMemoryManager());
		List<String[]> expectedArray = new ArrayList<>();
		
		for(int i = 0; i < REPEAT; i++) {
			expectedArray.add(new String[]{"a5", "c3"});
			expectedArray.add(new String[]{"a1", "c6"});
			expectedArray.add(new String[]{"a2", "c2"});
			expectedArray.add(new String[]{"a3", "c7"});
		}
		
		synthesizeTest("Test long projection", projection, expectedArray);
	}
	
	@Test
	public void testCorrectJoinOperatorWithMemory() {
		PhysicalOperator join = getJoinOperator(tp6.getMemoryManager());
		List<String[]> expectedArray = new ArrayList<>();
		
		expectedArray.add(new String[]{"a2", "b5", "c2", "e6", "d3"});
		expectedArray.add(new String[]{"a3", "b8", "c7", "e9", "d5"});
		expectedArray.add(new String[]{"a5", "b1", "c3", "e4", "d1"});
		
		synthesizeTest("Test join", join, expectedArray);
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
}
