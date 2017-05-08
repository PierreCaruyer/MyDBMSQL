package univlille.m1info.abd.tp7;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import univlille.m1info.abd.index.DefaultIndex;
import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.PhysicalOperator;
import univlille.m1info.abd.phys.SequentialAccessOnARelationOperator;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.tp3.TestTP3;
import univlille.m1info.abd.tp6.TestTP6;

public class TestTP7 {

	private MemoryManager mem;
	private SchemawithMemory sgbd;
	private String testedRelName = "REL";
	private String testedColName = "attrA";

	@Before
	public void setUp() {
		sgbd = new SchemawithMemory();
	}

	public void computeTest(RAQuery test, List<String[]> expected) {
		QueryEvaluator query = new QueryEvaluator(test, sgbd);
		List<String[]> actual = query.evaluate();
		assertTrue(containTheSameTuples(expected, actual));
	}
	
	@Test
	public void testMultiplePageIndex() {
		getLongRightTable();
		Index index = new DefaultIndex(testedRelName, testedColName, sgbd);
		sgbd.addIndex(testedRelName, testedColName, index);
		assertTrue(moreThanOneAddress(index.getListofAddresses(new String[] { "a5", "b1", "c3" })));
	}

	private boolean moreThanOneAddress(List<Integer> list) {
		int count = 0;
		Iterator<Integer> it = list.iterator();
		for(;it.hasNext();) {
			System.out.println(it.next());
			count++;
		}
		return count > 1;
	}
	/**
	 * Loads a table w/ many tuples to test memory allocation and free
	 * mecanisms' correctness
	 */
	private PhysicalOperator getLongRightTable() {
		sgbd.createRelation(testedRelName, new String[] { testedColName, "attrB", "attrC" });
		List<String[]> tuples = new ArrayList<>();

		for (int i = 0; i < TestTP6.REPEAT; i++) {
			tuples.add(new String[] { "a5", "b1", "c3" });
			tuples.add(new String[] { "a1", "b4", "c6" });
			tuples.add(new String[] { "a2", "b5", "c2" });
			tuples.add(new String[] { "a3", "b8", "c7" });
		}

		sgbd.FillRelation(testedRelName, tuples);

		return new SequentialAccessOnARelationOperator(sgbd.getRelation(testedRelName), mem);
	}

	@Test
	public void testCorrectProjection () {
		List<String[]> tuples = new ArrayList<>(), expected = new ArrayList<String[]>();
		DefaultRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		
		tuples.add(new String[]{"a1", "b1"});
		tuples.add(new String[]{"a2", "b2"});
		relation.loadTuples(tuples);
		
		expected.add(new String[]{"a1"});
		expected.add(new String[]{"a2"});
		computeTest(TestTP3.getQuery_project_attrA_from_REL(), expected);
	}

	@Test
	public void testProjectionWithEpmtyInput () {
		List<String[]> expected = new ArrayList<>(), tuples = new ArrayList<>();
		DefaultRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		relation.loadTuples(tuples);
		computeTest(TestTP3.getQuery_project_attrA_from_REL(), expected);
	}

	@Test
	public void testJoin() {
		List<String[]> expected = new ArrayList<>();
		expected.add(new String[] { "a1", "b1", "c1" });
		expected.add(new String[] { "a1", "b1", "c2" });

		// Create two relations
		DefaultRelation relation1 = sgbd.createRelation("REL", "attrA", "attrB");
		List<String[]> tuples1 = new ArrayList<>();
		tuples1.add(new String[]{"a1", "b1"});
		tuples1.add(new String[]{"a2", "b2"});
		relation1.loadTuples(tuples1);

		DefaultRelation relation2 = sgbd.createRelation("RELTWO", "attrA", "attrC");
		List<String[]> tuples2 = new ArrayList<>();
		tuples2.add(new String[]{"a1", "c1"});
		tuples2.add(new String[]{"a1", "c2"});
		relation2.loadTuples(tuples2);
		
		computeTest(TestTP3.getQuery_join_select_attrA_EQUAL_a1_from_REL_attrA_attrB_with_RELTWO_attrA_attrC(), expected);
	}

	@Test
	public void testCorrectSelection () {
		
		DefaultRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		List<String[]> tuples = new ArrayList<>(), expected = new ArrayList<>();
		tuples.add(new String[]{"a1", "b1"});
		tuples.add(new String[]{"a2", "b2"});
		relation.loadTuples(tuples);

		expected.add(new String[]{"a1", "b1"});
		computeTest(TestTP3.getQuery_select_attrA_EQUAL_a1_from_REL(), expected);
	}
	
	@Test
	public void testSelectionWithEmptyInput () {
		List<String[]> expected = new ArrayList<>();
		DefaultRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		relation.loadTuples(new ArrayList<String[]>());
		
		computeTest(TestTP3.getQuery_select_attrA_EQUAL_a1_from_REL(), expected);
	}

	@Test
	public void testSelectionWithEmptyResult () {
		DefaultRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		List<String[]> tuples = new ArrayList<>(), expected = new ArrayList<>();
		tuples.add(new String[]{"a3", "b1"});
		tuples.add(new String[]{"a2", "b2"});
		relation.loadTuples(tuples);

		computeTest(TestTP3.getQuery_select_attrA_EQUAL_a1_from_REL(), expected);
	}

	@Test
	public void testCorrectJoinOfTwoTables () {
		List<String[]> tuples1 = new ArrayList<>(), tuples2 = new ArrayList<>(), expected = new ArrayList<String[]>();
		DefaultRelation relation1 = sgbd.createRelation("RELONE", "attrA", "attrB");
		tuples1.add(new String[]{"a1", "b1"});
		tuples1.add(new String[]{"a2", "b2"});
		relation1.loadTuples(tuples1);

		DefaultRelation relation2 = sgbd.createRelation("RELTWO", "attrA", "attrC");
		tuples2.add(new String[]{"a1", "c1"});
		tuples2.add(new String[]{"a1", "c2"});
		relation2.loadTuples(tuples2);

		expected.add(new String[]{"a1", "b1", "c1"});
		expected.add(new String[]{"a1", "b1", "c2"});
		computeTest(TestTP3.getQuery_join_RELONE_attrA_attrB_with_RELTWO_attrA_attrC(), expected);
	}

	@Test
	public void testJoinWithEmptyRightInput() {
		DefaultRelation relation1 = sgbd.createRelation("RELONE", "attrA", "attrB");
		
		List<String[]> tuples = new ArrayList<>(), expected = new ArrayList<String[]>();
		tuples.add(new String[]{"a1", "b1"});
		tuples.add(new String[]{"a2", "b2"});
		relation1.loadTuples(tuples);

		computeTest(TestTP3.getQuery_join_RELONE_attrA_attrB_with_RELTWO_attrA_attrC(), expected);
	}


	@Test
	public void testJoinWithEmptyLeftInput() {
		List<String[]> tuples1 = new ArrayList<>(), tuples2 = new ArrayList<>(), expected = new ArrayList<>();
		
		DefaultRelation relation1 = sgbd.createRelation("RELONE", "attrA", "attrB");
		relation1.loadTuples(tuples1);

		DefaultRelation relation2 = sgbd.createRelation("RELTWO", "attrA", "attrC");
		tuples2.add(new String[]{"a1", "c1"});
		tuples2.add(new String[]{"a1", "c2"});
		relation2.loadTuples(tuples2);

		computeTest(TestTP3.getQuery_join_RELONE_attrA_attrB_with_RELTWO_attrA_attrC(), expected);
	}

	@Test
	public void testJoinWithEmptyResult() {
		List<String[]> tuples1 = new ArrayList<>(), tuples2 = new ArrayList<>(), expected = new ArrayList<>();
		
		DefaultRelation relation1 = sgbd.createRelation("RELONE", "attrA", "attrB");
		tuples1.add(new String[]{"a3", "b1"});
		tuples1.add(new String[]{"a2", "b2"});
		relation1.loadTuples(tuples1);

		DefaultRelation relation2 = sgbd.createRelation("RELTWO", "attrA", "attrC");
		tuples2.add(new String[]{"a1", "c1"});
		tuples2.add(new String[]{"a1", "c2"});
		relation2.loadTuples(tuples2);

		computeTest(TestTP3.getQuery_join_RELONE_attrA_attrB_with_RELTWO_attrA_attrC(), expected);
	}

	private boolean containTheSameTuples (List<String[]> list1, List<String[]> list2) {
		for (String[] tuple: list1) {
			if (! listContains(list2, tuple))
				return false;
		}
		for (String[] tuple: list2) {
			if (! listContains(list1, tuple))
				return false;
		}
		return true;
	}

	private boolean listContains (List<String[]> list, String[] tuple) {
		for (String[] el: list)
			if (Arrays.equals(el, tuple))
				return true;
		return false;
	}

}

