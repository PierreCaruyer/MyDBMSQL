package univlille.m1info.abd.tp3;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import univlille.m1info.abd.phys.old.PhysicalOperator;
import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.ra.JoinQuery;
import univlille.m1info.abd.ra.ProjectionQuery;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RelationNameQuery;
import univlille.m1info.abd.ra.SelectionQuery;
import univlille.m1info.abd.simplebd.SimpleDBRelation;
import univlille.m1info.abd.simplebd.SimpleSGBD;

public class TestTP3 {

	private TP3 tp3;
	@Before
	public void setUp () {
		this.tp3 = new TP3();
	}
	
	public static RAQuery getQuery_select_attrA_EQUAL_a1_from_REL() {
		RAQuery relNameQuery = new RelationNameQuery("REL");
		RAQuery selection = new SelectionQuery(relNameQuery, "attrA", ComparisonOperator.EQUAL, "a1");
		return selection;
	}
	
	public static RAQuery getQuery_project_attrA_from_REL() {
		RAQuery relNameQuery = new RelationNameQuery("REL");
		RAQuery projection = new ProjectionQuery(relNameQuery, "attrA");
		return projection;
	}
	
	public static RAQuery getQuery_join_RELONE_attrA_attrB_with_RELTWO_attrA_attrC() {
		RAQuery relNameOneQuery = new RelationNameQuery("RELONE");
		RAQuery relNameTwoQuery = new RelationNameQuery("RELTWO");
		RAQuery join = new JoinQuery(relNameOneQuery, relNameTwoQuery);
		return join;
	}
	
	public static RAQuery getQuery_join_select_attrA_EQUAL_a1_from_REL_attrA_attrB_with_RELTWO_attrA_attrC () {
		RAQuery leftSubQuery = getQuery_select_attrA_EQUAL_a1_from_REL();
		RAQuery relNameTwoQuery = new RelationNameQuery("RELTWO");
		RAQuery join = new JoinQuery(leftSubQuery, relNameTwoQuery);
		return join;
	}

	
	@Test
	public void testCorrectProjection () {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create a relation with two tuples
		SimpleDBRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		relation.addTuple(new String[]{"a1", "b1"});
		relation.addTuple(new String[]{"a2", "b2"});
		
		// The query that we want to compute: PROJECT[attA](REL)
		RAQuery query = getQuery_project_attrA_from_REL();
		
		// The operator that will compute the query
		PhysicalOperator operator = tp3.getOperator(query, sgbd);
		
		// Check that the operator returns the correct result
		List<String[]> expectedResult = new ArrayList<String[]>();
		expectedResult.add(new String[]{"a1"});
		expectedResult.add(new String[]{"a2"});
		List<String[]> obtainedResult = collectAllTuplesOfAnOperator(operator);
		if (! containTheSameTuples(expectedResult, obtainedResult)) {
			fail("Wrong result. Expected " + listToString(expectedResult) + " Obtained " + listToString(obtainedResult));
		}
	}

	@Test
	public void testProjectionWithEpmtyInput () {
	
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create a relation with two tuples
		sgbd.createRelation("REL", "attrA", "attrB");
		
		// The query that we want to compute: PROJECT[attA](REL)
		RAQuery query = getQuery_project_attrA_from_REL();
		
		// The operator that will compute the query
		PhysicalOperator operator = tp3.getOperator(query, sgbd);
		
		// Check that the result is empty
		assertNull(operator.nextTuple());
	}
	
	@Test
	public void testCorrectSelection () {
		
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create a relation with two tuples
		SimpleDBRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		relation.addTuple(new String[]{"a1", "b1"});
		relation.addTuple(new String[]{"a2", "b2"});
		
		// The query that we want to compute: SELECT[attrA="a1"](REL)
		RAQuery query = getQuery_select_attrA_EQUAL_a1_from_REL();
		
		// The operator that will compute the query
		PhysicalOperator operator = tp3.getOperator(query, sgbd);
		
		// Check that the operator returns the correct result
		List<String[]> expectedResult = new ArrayList<String[]>();
		expectedResult.add(new String[]{"a1", "b1"});
		List<String[]> obtainedResult = collectAllTuplesOfAnOperator(operator);
		if (! containTheSameTuples(expectedResult, obtainedResult)) {
			fail("Wrong result. Expected " + listToString(expectedResult) + " Obtained " + listToString(obtainedResult));
		}
	}
	
	@Test
	public void testSelectionWithEmptyInput () {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create a relation with two tuples
		sgbd.createRelation("REL", "attrA", "attrB");
		
		// The query that we want to compute: SELECT[attrA="a1"](REL)
		RAQuery query = getQuery_select_attrA_EQUAL_a1_from_REL();
				
		// The operator that will compute the query
		PhysicalOperator operator = tp3.getOperator(query, sgbd);
		
		// Check that the result is empty
		assertNull(operator.nextTuple());
	}
	
	@Test
	public void testSelectionWithEmptyResult () {
		
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create a relation with two tuples
		SimpleDBRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		relation.addTuple(new String[]{"a3", "b1"});
		relation.addTuple(new String[]{"a2", "b2"});
		
		// The query that we want to compute: SELECT[attrA="a1"](REL)
		RAQuery query = getQuery_select_attrA_EQUAL_a1_from_REL();
		
		// The operator that will compute the query
		PhysicalOperator operator = tp3.getOperator(query, sgbd);	
		
		// Check that the result is empty
		assertNull(operator.nextTuple());
	}
	
	@Test
	public void testCorrectJoinOfTwoTables () {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create two relations
		SimpleDBRelation relation1 = sgbd.createRelation("RELONE", "attrA", "attrB");
		relation1.addTuple(new String[]{"a1", "b1"});
		relation1.addTuple(new String[]{"a2", "b2"});
		
		SimpleDBRelation relation2 = sgbd.createRelation("RELTWO", "attrA", "attrC");
		relation2.addTuple(new String[]{"a1", "c1"});
		relation2.addTuple(new String[]{"a1", "c2"});
		
		// The query that we want to compute: RELONE JOIN RELTWO
		RAQuery query = getQuery_join_RELONE_attrA_attrB_with_RELTWO_attrA_attrC();
		
		// The operator that will compute the query
		PhysicalOperator operator = tp3.getOperator(query, sgbd);
		
		// Check that the operator returns the correct result
		List<String[]> expectedResult = new ArrayList<String[]>();
		expectedResult.add(new String[]{"a1", "b1", "c1"});
		expectedResult.add(new String[]{"a1", "b1", "c2"});
		List<String[]> obtainedResult = collectAllTuplesOfAnOperator(operator);
		if (! containTheSameTuples(expectedResult, obtainedResult)) {
			fail("Wrong result. Expected " + listToString(expectedResult) + " Obtained " + listToString(obtainedResult));
		}
	}
	
	@Test
	public void testJoinWithEmptyRightInput() {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create two relations
		SimpleDBRelation relation1 = sgbd.createRelation("RELONE", "attrA", "attrB");
		relation1.addTuple(new String[]{"a1", "b1"});
		relation1.addTuple(new String[]{"a2", "b2"});
		
		sgbd.createRelation("RELTWO", "attrA", "attrC");
		
		// The query that we want to compute: RELONE JOIN RELTWO
		RAQuery query = getQuery_join_RELONE_attrA_attrB_with_RELTWO_attrA_attrC();
		
		// The operator that will compute the query
		PhysicalOperator operator = tp3.getOperator(query, sgbd);
		
		// Check that the result is empty
		assertNull(operator.nextTuple());
	}
	

	@Test
	public void testJoinWithEmptyLeftInput() {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create two relations
		sgbd.createRelation("RELONE", "attrA", "attrB");
		
		SimpleDBRelation relation2 = sgbd.createRelation("RELTWO", "attrA", "attrC");
		relation2.addTuple(new String[]{"a1", "c1"});
		relation2.addTuple(new String[]{"a1", "c2"});
		
		// The query that we want to compute: RELONE JOIN RELTWO
		RAQuery query = getQuery_join_RELONE_attrA_attrB_with_RELTWO_attrA_attrC();
		
		// The operator that will compute the query
		PhysicalOperator operator = tp3.getOperator(query, sgbd);
		
		// Check that the result is empty
		assertNull(operator.nextTuple());
	}

	@Test
	public void testJoinWithEmptyResult() {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create two relations
		SimpleDBRelation relation1 = sgbd.createRelation("RELONE", "attrA", "attrB");
		relation1.addTuple(new String[]{"a3", "b1"});
		relation1.addTuple(new String[]{"a2", "b2"});
		
		SimpleDBRelation relation2 = sgbd.createRelation("RELTWO", "attrA", "attrC");
		relation2.addTuple(new String[]{"a1", "c1"});
		relation2.addTuple(new String[]{"a1", "c2"});
		
		// The query that we want to compute: RELONE JOIN RELTWO
		RAQuery query = getQuery_join_RELONE_attrA_attrB_with_RELTWO_attrA_attrC();
		
		// The operator that will compute the query
		PhysicalOperator operator = tp3.getOperator(query, sgbd);
		
		// Check that the result is empty
		assertNull(operator.nextTuple());
	}

	// Utility method
	private List<String[]> collectAllTuplesOfAnOperator (PhysicalOperator operator) {
		List<String[]> tuples = new ArrayList<>();
		String[] rt;
		while ((rt = operator.nextTuple()) != null) {
			tuples.add(rt);
		}
		return tuples;
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
	
	// Utility method
	private String listToString (List<String[]> list) {
		StringBuilder s = new StringBuilder();
		for (String[] el : list) {
			s.append(Arrays.toString(el) + ",");
		}
		if (s.length() == 0)
			return "[]";
		return "[" + s.toString().substring(0, s.length()-1) + "]";
	}
	
	private boolean listContains (List<String[]> list, String[] tuple) {
		for (String[] el: list)
			if (Arrays.equals(el, tuple))
				return true;
		return false;
	}

}
