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

public class TestTP6 {

	public static final int PAGE_SIZE = 20;
	public static final int ATTRIBUTE_SIZE = 20;
	public static final int REPEAT = 15;
	private DisposalRelations relations;
	private MemoryManager mem;
	private TP6 tp6;
	
	@Before
	public void setUp() {
		tp6 = new TP6();
		mem = tp6.getMemoryManager();
		relations = new DisposalRelations(mem);
	}

	private void synthesizeTest(String testName, PhysicalOperator testOperator, List<String[]> expectedTuples, boolean finalTest) {
		if(finalTest)
			System.out.println("----------" + testName + "----------");
		try {
			List<String[]> tupleArray = tp6.getOperatorTuples(testOperator);
			System.out.println("Number of reads : " + mem.getNumberOfDiskReadSinceLastReset());
			System.out.println("Number of writes : " + mem.getNumberofWriteDiskSinceLastReset());
			assertEquals(expectedTuples.size(), tupleArray.size());
			assertTrue(pageContentEquals(expectedTuples, tupleArray));
		} catch (NotEnoughMemoryException e) {
			fail();
		}
	}

	@Test
	public void testCorrectShortSelectionOperatorWithMemory() {
		PhysicalOperator selection = relations.getShortSelectionOperator();
		List<String[]> expectedArray = new ArrayList<>();
		expectedArray.add(new String[] { "a5", "b1", "c3" });
		synthesizeTest("Test short selection", selection, expectedArray, true);
	}

	@Test
	public void testCorrectLongSelectionOperatorWithMemory() {
		PhysicalOperator selection = relations.getLongSelectionOperator();
		List<String[]> expectedArray = new ArrayList<>();
		for (int i = 0; i < REPEAT; i++)
			expectedArray.add(new String[] { "a5", "b1", "c3" });
		synthesizeTest("Test long selection", selection, expectedArray, true);
	}

	@Test
	public void testCorrectShortProjectionOperatorWithMemory() {
		PhysicalOperator projection = relations.getShortProjectionOperator();
		List<String[]> expectedArray = new ArrayList<>();

		expectedArray.add(new String[] { "a5", "c3" });
		expectedArray.add(new String[] { "a1", "c6" });
		expectedArray.add(new String[] { "a2", "c2" });
		expectedArray.add(new String[] { "a3", "c7" });

		synthesizeTest("Test short projection", projection, expectedArray, true);
	}

	@Test
	public void testCorrectLongProjectionOperatorWithMemory() {
		PhysicalOperator projection = relations.getLongProjectionOperator();
		List<String[]> expectedArray = new ArrayList<>();

		for (int i = 0; i < REPEAT; i++) {
			expectedArray.add(new String[] { "a5", "c3" });
			expectedArray.add(new String[] { "a1", "c6" });
			expectedArray.add(new String[] { "a2", "c2" });
			expectedArray.add(new String[] { "a3", "c7" });
		}

		synthesizeTest("Test long projection", projection, expectedArray, true);
	}

	@Test
	public void testCorrectJoinOperatorWithMemory() {
		PhysicalOperator join = relations.getJoinOperator();
		List<String[]> expectedArray = new ArrayList<>();

		expectedArray.add(new String[] { "a2", "b5", "c2", "e6", "d3" });
		expectedArray.add(new String[] { "a3", "b8", "c7", "e9", "d5" });
		expectedArray.add(new String[] { "a5", "b1", "c3", "e4", "d1" });

		synthesizeTest("Test join", join, expectedArray, true);
	}

	
	/**
	 * Just testing free / allocation mecanism here, not hoping to get a result
	 * equivalent to the expectedArray
	 */
	@Test
	public void testCorrectLongJoinOperatorWithMemory() {
		PhysicalOperator join = relations.getLongJoinOperator();
		List<String[]> expectedArray = new ArrayList<>();

		for (int i = 0; i < REPEAT * (REPEAT / 2 + 1); i++) {
			expectedArray.add(new String[] { "a2", "b5", "c2", "e6", "d3" });
			expectedArray.add(new String[] { "a3", "b8", "c7", "e9", "d5" });
			expectedArray.add(new String[] { "a5", "b1", "c3", "e4", "d1" });
		}

		synthesizeTest("Test Long join", join, expectedArray, true);
	}
	
	/**
	 * Testing operator combinations
	 */
	@Test
	public void testProjectionAfterSelection() {
		PhysicalOperator sel = relations.getLongSelectionOperator();
		PhysicalOperator proj = new ProjectionOperator(sel, mem, "attrA", "attrC");
		
		List<String[]> intermediaryResult = relations.getProjectionAfterSelectionExpectedTuples();
		
		synthesizeTest("", sel, intermediaryResult, false);
		sel.reset();
		
		List<String[]> expectedArray = new ArrayList<>();
		for (int i = 0; i < REPEAT; i++)
			expectedArray.add(new String[] { "a5", "c3" });
		synthesizeTest("Test projection on a selection", proj, expectedArray, true);
	}
	
	@Test
	public void testSelectionAfterProjection() {
		PhysicalOperator proj = relations.getProjectionOnBAndCAttributes();
		PhysicalOperator sel = new SelectionOperator(proj, "attrC", "c7", ComparisonOperator.EQUAL, mem);
		
		List<String[]> intermediaryResult = relations.getSelectionAfterProjectionExpectedTuples();
		
		synthesizeTest("", proj, intermediaryResult, false);
		proj.reset();
		
		List<String[]> expectedArray = new ArrayList<>();
		for (int i = 0; i < REPEAT; i++)
			expectedArray.add(new String[] { "b8", "c7" });
		synthesizeTest("Test selection on a projection", sel, expectedArray, true);
	}
	
	@Test
	public void testParcoursTable() throws IOException, NotEnoughMemoryException {
		MemoryManager mem = new SimpleMemoryManager(2, 2);
		SequentialAccessOnARelationOperator tableOp = relations.getLeftModTable(mem);
		int testNbTuples = 0;
		int testNbPages = 0;

		int pageNb;
		while ((pageNb = tableOp.nextPage()) != -1) {
			testNbPages++;
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();
			for (String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple())
				testNbTuples++;
			mem.releasePage(pageNb, false);
		}
		assertEquals(9, testNbTuples);
		assertEquals(5, testNbPages);
	}

	@Test
	public void testSelection1() throws IOException, NotEnoughMemoryException {
		MemoryManager mem = new SimpleMemoryManager(100, 2);
		SequentialAccessOnARelationOperator tableOp = relations.getLeftModTable(mem);
		SelectionOperator sel = new SelectionOperator(tableOp, "ra", "a1", ComparisonOperator.EQUAL, mem);

		int pageNb;
		while ((pageNb = sel.nextPage()) != -1) {
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();

			for (String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple()) ;
			mem.releasePage(pageNb, false);
		}
		System.out.println("Number of operations : " + mem.getNumberOfDiskReadSinceLastReset());
		System.out.println("RESET");

		List<String[]> result = new ArrayList<>();
		sel.reset();
		while ((pageNb = sel.nextPage()) != -1) {
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();
			for (String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple())
				result.add(tuple);
			mem.releasePage(pageNb, false);
		}
		assertEquals(relations.getSeleection1ExpectedTuples().size(), result.size());
	}

	@Test
	public void testProjection1() throws IOException, NotEnoughMemoryException {
		MemoryManager mem = new SimpleMemoryManager(2, 2);
		SequentialAccessOnARelationOperator tableOp = relations.getLeftModTable(mem);
		ProjectionOperator proj = new ProjectionOperator(tableOp, mem, "ra");

		int pageNb;
		while ((pageNb = proj.nextPage()) != -1) {
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();
			for (String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple()) ;
			mem.releasePage(pageNb, false);
		}

		System.out.println("Number of operations : " + mem.getNumberOfDiskReadSinceLastReset());
		System.out.println("RESET");

		List<String[]> result = new ArrayList<>();
		proj.reset();
		while ((pageNb = proj.nextPage()) != -1) {
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();
			for (String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple())
				result.add(tuple);
			mem.releasePage(pageNb, false);
		}
		assertEquals(relations.getProjection1ExpectedTuples().size(), result.size());

	}

	@Test
	public void testJoin() throws IOException, NotEnoughMemoryException {
		System.out.println("join operator");
		SequentialAccessOnARelationOperator tableOpLeft = relations.getLeftModTable(mem);
		SequentialAccessOnARelationOperator tableOpRight = relations.getRightModTable(mem);

		List<String[]> resultArray = relations.getExpectedResultJoinTuples();
		
		JoinOperator join = new JoinOperator(tableOpLeft, tableOpRight, mem);
		int pageNb;
		while ((pageNb = join.nextPage()) != -1) {
			Page page = mem.loadPage(pageNb);
			page.switchToReadMode();
			for (String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple()) ;
			mem.releasePage(pageNb, false);
		}

		System.out.println("Number of operations : " + mem.getNumberOfDiskReadSinceLastReset());
		System.out.println("RESET");

		List<String[]> result = new ArrayList<>();
		join.reset();
		pageNb = join.nextPage();
		while (pageNb != -1) {
			Page page = mem.loadPage(pageNb);
			
			page.switchToReadMode();
			for (String[] tuple = page.nextTuple(); tuple != null; tuple = page.nextTuple()) {
				System.out.println(Arrays.toString(tuple));
				result.add(tuple);
			}
			mem.releasePage(pageNb, false);
			pageNb = join.nextPage();
		}
		for(String[] array : resultArray)
			System.out.println(Arrays.toString(array));

		assertEquals(resultArray.size(), result.size());
		assertTrue(pageContentEquals(resultArray, result));
	}

	public boolean pageContentEquals(List<String[]> expected, List<String[]> actual) {
		for (int i = 0; i < expected.size(); i++) {
			String[] tuple = expected.get(i);
			String[] actualTuple = expected.get(i);
			for (int j = 0; j < tuple.length; j++)
				if (!tuple[j].equals(actualTuple[j]))
					return false;
		}
		return true;
	}
}
