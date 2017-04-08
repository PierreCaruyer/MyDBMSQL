package univlille.m1info.abd.tp6;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.phys.JoinOperator;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.phys.PhysicalOperator;
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
	private static final int REPEAT = 15;
	private MemoryManager mem;
	private TP6 tp6;

	/**
	 * Loads a Short table
	 */
	public PhysicalOperator getRightLoadedTable() {
		RelationSchema schema = new DefaultRelationSchema("RELONE", new String[] { "attrA", "attrB", "attrC" });
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();

		tuples.add(new String[] { "a5", "b1", "c3" });
		tuples.add(new String[] { "a1", "b4", "c6" });
		tuples.add(new String[] { "a2", "b5", "c2" });
		tuples.add(new String[] { "a3", "b8", "c7" });

		relation.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(relation, mem);
	}

	/**
	 * Loads a Short table
	 */
	public PhysicalOperator getLeftLoadedTable() {
		RelationSchema schema = new DefaultRelationSchema("RELTWO", new String[] { "attrE", "attrD", "attrA" });
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();

		tuples.add(new String[] { "e4", "d1", "a5" });
		tuples.add(new String[] { "e6", "d4", "a4" });
		tuples.add(new String[] { "e9", "d5", "a3" });
		tuples.add(new String[] { "e6", "d3", "a2" });

		relation.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(relation, mem);
	}

	/**
	 * Loads a table w/ many tuples to test memory allocation and free
	 * mecanisms' correctness
	 */
	public PhysicalOperator getLongRightTable() {
		RelationSchema schema = new DefaultRelationSchema("RELLONGR", new String[] { "attrA", "attrB", "attrC" });
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();

		for (int i = 0; i < REPEAT; i++) {
			tuples.add(new String[] { "a5", "b1", "c3" });
			tuples.add(new String[] { "a1", "b4", "c6" });
			tuples.add(new String[] { "a2", "b5", "c2" });
			tuples.add(new String[] { "a3", "b8", "c7" });
		}

		relation.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(relation, mem);
	}
	
	public PhysicalOperator getLongLeftTable() {
		RelationSchema schema = new DefaultRelationSchema("RELLONGR", new String[] { "attrA", "attrB", "attrC" });
		DefaultRelation relation = new DefaultRelation(schema, mem);
		List<String[]> tuples = new ArrayList<>();

		for (int i = 0; i < REPEAT; i++) {
			tuples.add(new String[] { "e4", "d1", "a5" });
			tuples.add(new String[] { "e6", "d4", "a4" });
			tuples.add(new String[] { "e9", "d5", "a3" });
			tuples.add(new String[] { "e6", "d3", "a2" });
		}

		relation.loadTuples(tuples);

		return new SequentialAccessOnARelationOperator(relation, mem);
	}
	
	// Selection operator w/ few tuples
	public PhysicalOperator getShortSelectionOperator() {
		return new SelectionOperator(getRightLoadedTable(), "attrA", "a5", ComparisonOperator.EQUAL, mem);
	}

	// Selection operator w/ more tuples
	public PhysicalOperator getLongSelectionOperator() {
		return new SelectionOperator(getLongRightTable(), "attrA", "a5", ComparisonOperator.EQUAL, mem);
	}

	// Projection operator w/ few tuples
	public PhysicalOperator getShortProjectionOperator() {
		return new ProjectionOperator(getRightLoadedTable(), mem, new String[] { "attrA", "attrC" });
	}

	// Projection operator w/ more tuples
	public PhysicalOperator getLongProjectionOperator() {
		return new ProjectionOperator(getLongRightTable(), mem, new String[] { "attrA", "attrC" });
	}

	public PhysicalOperator getJoinOperator() {
		return new JoinOperator(getRightLoadedTable(), getLeftLoadedTable(), mem);
	}
	
	public PhysicalOperator getLongJoinOperator() {
		return new JoinOperator(getLongRightTable(), getLongLeftTable(), mem);
	}

	@Before
	public void setUp() {
		tp6 = new TP6(PAGE_SIZE, ATTRIBUTE_SIZE);
		mem = tp6.getMemoryManager();
	}

	private void synthesizeTest(String testName, PhysicalOperator testOperator, List<String[]> expectedTuples) {
		System.out.println(testName);
		try {
			List<String[]> tupleArray = tp6.getOperatorTuples(testOperator);
			//System.out.println("Number of reads : " + mem.getNumberOfDiskReadSinceLastReset());
			if(testOperator instanceof JoinOperator) {
				tp6.displayPageContent(tupleArray);
				System.out.println(tupleArray.size());
			}
			assertTrue(pageContentEquals(expectedTuples, tupleArray));
		} catch (NotEnoughMemoryException e) {
			fail();
		}
	}

	/*@Test
	public void testCorrectShortSelectionOperatorWithMemory() {
		PhysicalOperator selection = getShortSelectionOperator();
		List<String[]> expectedArray = new ArrayList<>();
		expectedArray.add(new String[] { "a5", "b1", "c3" });
		synthesizeTest("Test short selection", selection, expectedArray);
	}

	@Test
	public void testCorrectLongSelectionOperatorWithMemory() {
		PhysicalOperator selection = getLongSelectionOperator();
		List<String[]> expectedArray = new ArrayList<>();
		for (int i = 0; i < REPEAT; i++)
			expectedArray.add(new String[] { "a5", "b1", "c3" });
		synthesizeTest("Test long selection", selection, expectedArray);
	}

	@Test
	public void testCorrectShortProjectionOperatorWithMemory() {
		PhysicalOperator projection = getShortProjectionOperator();
		List<String[]> expectedArray = new ArrayList<>();

		expectedArray.add(new String[] { "a5", "c3" });
		expectedArray.add(new String[] { "a1", "c6" });
		expectedArray.add(new String[] { "a2", "c2" });
		expectedArray.add(new String[] { "a3", "c7" });

		synthesizeTest("Test short projection", projection, expectedArray);
	}

	@Test
	public void testCorrectLongProjectionOperatorWithMemory() {
		PhysicalOperator projection = getLongProjectionOperator();
		List<String[]> expectedArray = new ArrayList<>();

		for (int i = 0; i < REPEAT; i++) {
			expectedArray.add(new String[] { "a5", "c3" });
			expectedArray.add(new String[] { "a1", "c6" });
			expectedArray.add(new String[] { "a2", "c2" });
			expectedArray.add(new String[] { "a3", "c7" });
		}

		synthesizeTest("Test long projection", projection, expectedArray);
	}

	@Test
	public void testCorrectJoinOperatorWithMemory() {
		PhysicalOperator join = getJoinOperator();
		List<String[]> expectedArray = new ArrayList<>();

		expectedArray.add(new String[] { "a2", "b5", "c2", "e6", "d3" });
		expectedArray.add(new String[] { "a3", "b8", "c7", "e9", "d5" });
		expectedArray.add(new String[] { "a5", "b1", "c3", "e4", "d1" });

		synthesizeTest("Test join", join, expectedArray);
	}*/
	
	@Test
	/**
	 * Just testing free / allocation mecanism here, not hoping to get a result
	 * equivalent to the expectedArray
	 */
	public void testCorrectLongJoinOperatorWithMemory() {
		PhysicalOperator join = getLongJoinOperator();
		List<String[]> expectedArray = new ArrayList<>();

		for(int i = 0; i < REPEAT; i++) {
			expectedArray.add(new String[] { "a2", "b5", "c2", "e6", "d3" });
			expectedArray.add(new String[] { "a3", "b8", "c7", "e9", "d5" });
			expectedArray.add(new String[] { "a5", "b1", "c3", "e4", "d1" });
		}

		synthesizeTest("Test Long join", join, expectedArray);
	}

	public boolean pageContentEquals(List<String[]> expected, List<String[]> actual) {
		if (expected.size() != actual.size())
			return false;
		for (int i = 0; i < expected.size(); i++) {
			String[] tuple = expected.get(i);
			String[] actualTuple = expected.get(i);
			for (int j = 0; j < tuple.length; j++) {
				if (!tuple[j].equals(actualTuple[j]))
					return false;
			}
		}
		return true;
	}
	
	
	/*
	 * TESTS MME BONEVA
	 */
	
	
	@Test
	public void testParcoursTable () throws IOException, NotEnoughMemoryException {
		
		RelationSchema schema = new DefaultRelationSchema("REL", "ra", "rb");
		MemoryManager mem = new SimpleMemoryManager(5, 2);
		DefaultRelation rel = new DefaultRelation(schema, mem);
		
		ArrayList<String[]> tuples = new ArrayList<>();
		for (int i = 1; i <=9; i++) {
			tuples.add(new String[]{"a"+(i%3), "b"+i});	
		}
		
		rel.loadTuples(tuples);
		
		
		SequentialAccessOnARelationOperator tableOp = new SequentialAccessOnARelationOperator(rel, mem);
		int testNbTuples = 0;
		int testNbPages = 0;
		
		int pageNb;
		while ((pageNb = tableOp.nextPage()) != -1) {
			testNbPages++;
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();
			String[] tuple;
			while ((tuple = page.nextTuple()) != null) {
				System.out.println(Arrays.toString(tuple));
				testNbTuples++;
			}
			mem.releasePage(pageNb, false);
		}
		assertEquals(9, testNbTuples);
		assertEquals(4, testNbPages);
	}
	
	
	// Remplacer FiltreSelection par la classe du TP6 - Section 6 Q1 
	// Sachant qu'ici le filtrage est une simple sélection
	
	@Test
	public void testSelection1 () throws IOException, NotEnoughMemoryException {
				
		RelationSchema schema = new DefaultRelationSchema("REL", "ra", "rb");
		MemoryManager mem = new SimpleMemoryManager(100, 2);  
		DefaultRelation rel = new DefaultRelation(schema, mem);
		
		ArrayList<String[]> tuples = new ArrayList<>();
		for (int i = 1; i <=9; i++) {
			tuples.add(new String[]{"a"+(i%3), "b"+i});	
		}
		
		rel.loadTuples(tuples);		
		
		SequentialAccessOnARelationOperator tableOp = new SequentialAccessOnARelationOperator(rel, mem);
		SelectionOperator sel = new SelectionOperator(tableOp, "ra", "a1", ComparisonOperator.EQUAL, mem);
		
		int pageNb;
		while ((pageNb = sel.nextPage()) != -1) {
			System.out.println(pageNb);
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();
			String[] tuple;
			while ((tuple = page.nextTuple()) != null) {
				System.out.println(Arrays.toString(tuple));
			}
			mem.releasePage(pageNb, false);
		}
		System.out.println("Number of operations : " + mem.getNumberOfDiskReadSinceLastReset());
		
		
		System.out.println("RESET");
		
		List<String[]> result = new ArrayList<>();
		sel.reset();
		while ((pageNb = sel.nextPage()) != -1) {
			System.out.println(pageNb);
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();
			String[] tuple;
			while ((tuple = page.nextTuple()) != null) {
				result.add(tuple);
				System.out.println(Arrays.toString(tuple));
			}
			mem.releasePage(pageNb, false);
		}
		assertEquals(3, result.size());
		
	}
	
	// Remplacer JoinWithPages par la classe de TP6 - Section 7 Q2
	
	
	@Test
	public void testJoin() throws IOException, NotEnoughMemoryException {
		
		RelationSchema schema1 = new DefaultRelationSchema("RELONE", "ra", "rb");
		RelationSchema schema2 = new DefaultRelationSchema("RELTWO", "ra", "rc");
		MemoryManager mem = new SimpleMemoryManager(100, 2);
		
		ArrayList<String[]> tuples1 = new ArrayList<>();
		for (int i = 1; i <=9; i++) {
			tuples1.add(new String[]{"a"+(i%3), "b"+i});	
		}
		
		DefaultRelation rel1 = new DefaultRelation(schema1, mem);
		rel1.loadTuples(tuples1);
		
		
		ArrayList<String[]> tuples2 = new ArrayList<>();
		for (int i = 1; i <=9; i++) {
			tuples2.add(new String[]{"a"+(i%3), "c"+i});	
		}

		DefaultRelation rel2 = new DefaultRelation(schema2, mem);
		rel2.loadTuples(tuples2);
		
		SequentialAccessOnARelationOperator tableOpLeft = new SequentialAccessOnARelationOperator(rel1, mem);
		SequentialAccessOnARelationOperator tableOpRight = new SequentialAccessOnARelationOperator(rel2, mem);
		
		List<String[]> result = new ArrayList<>();
		
		JoinOperator join = new JoinOperator(tableOpLeft, tableOpRight, mem);
		int pageNb;
		while ((pageNb = join.nextPage()) != -1) {
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();
			String[] tuple;
			while ((tuple = page.nextTuple()) != null) {
				System.out.println(Arrays.toString(tuple));
				result.add(tuple);
			}
			mem.releasePage(pageNb, false);
		}

		assertEquals(27, result.size());
		
		System.out.println("Number of operations: " + mem.getNumberOfDiskReadSinceLastReset());
	}
}
