package univlille.m1info.abd.tp2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class TestTP2 {

	private TP2 tp2;
	
	@Before
	public void setUp () {
		this.tp2 = new TP2();
	}
	
	@Test
	public void testCorrectProjection () {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create a relation with two tuples
		SimpleDBRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		relation.addTuple(new String[]{"a1", "b1"});
		relation.addTuple(new String[]{"a2", "b2"});

		// Apply a projection operation PROJ[attrA] on this relation
		String resultRelName = tp2.computeProjection(sgbd, "REL", new String[]{"attrA"});
		
		// Check that the result relation has "attrA" as unique attribute
		assertArrayEquals(new String[]{"attrA"}, sgbd.getRelation(resultRelName).getRelationSchema().getSort());
		
		// Check that the result relation contains the two projected tuples
		// These tuples can appear in any order
		if (! tuplesOfRelationAre(sgbd, resultRelName, new String[]{"a1"}, new String[]{"a2"})) {
			String failMessage = String.format("Wrong result. Expected: [[a1],[a2]]. Found %s", 
					listToString(collectAllTuplesOfARelation(sgbd, resultRelName)));
			fail(failMessage);
		}
	}

	@Test(expected=Exception.class)
	public void testProjectionWithError () {
		SimpleSGBD sgbd = new SimpleSGBD();	
		
		// Create a relation with two tuples
		SimpleDBRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		relation.addTuple(new String[]{"a1", "b1"});
		relation.addTuple(new String[]{"a2", "b2"});

		// Apply a projection operation PROJ[noattr] on this relation, with non existing attribute
		// then try to retrieve a tuple in the resulting relation.
		// One of these operations (computing the projection or retrieving a tuple) should provoke an error
		// as the projection is ill defined
		
		String resultRelName = tp2.computeProjection(sgbd, "REL", new String[]{"noattr"});

		SimpleDBRelation resultRelation = sgbd.getRelation(resultRelName);
		resultRelation.switchToReadMode();
		resultRelation.nextTuple();
	}
	
	@Test
	public void testCorrectSelection() {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create a relation with two tuples
		SimpleDBRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		relation.addTuple(new String[]{"a1", "b1"});
		relation.addTuple(new String[]{"a2", "b2"});
		
		
		// Apply a selection operation SELECT[attrA="a1"] on this relation
		String resultRelName = tp2.computeSelection(sgbd, "REL", "attrA", "a1");
		
		// Check the correct relation description of the result: the relation description is the same as the one of the input relation
		assertArrayEquals(sgbd.getRelation("REL").getRelationSchema().getSort(),
							sgbd.getRelation(resultRelName).getRelationSchema().getSort());
		
		// Check that the result relation contains the two projected tuples
		// These tuples can appear in any order
		
		// Check that the result relation contains the one selected tuple only
		if (! tuplesOfRelationAre(sgbd, resultRelName, new String[]{"a1", "b1"})) {
			String failMessage = String.format("Wrong result. Expected: [[a1, b1]]. Found %s", 
					listToString(collectAllTuplesOfARelation(sgbd, resultRelName)));
			fail(failMessage);
		}
	}

	@Test(expected=Exception.class)
	public void testSelectionWithError () {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create a relation with two tuples
		SimpleDBRelation relation = sgbd.createRelation("REL", "attrA", "attrB");
		relation.addTuple(new String[]{"a1", "b1"});
		relation.addTuple(new String[]{"a2", "b2"});
		
		// Apply a selection operation SELECT[noattr] on this relation, with non existing attribute
		// then try to retrieve a tuple in the resulting relation.
		// One of these operations (computing the projection or retrieving a tuple) should provoke an error
		// as the projection is ill defined
		
		String resultRelName = tp2.computeSelection(sgbd, "REL", "noattr", "a1");

		SimpleDBRelation resultRelation = sgbd.getRelation(resultRelName);
		resultRelation.switchToReadMode();
		//resultRelation.nextTuple();
		
		System.out.println(listToString(collectAllTuplesOfARelation(sgbd, resultRelName)));
	}
	
	@Test
	public void testComputeCorrectJoin () {
		SimpleSGBD sgbd = new SimpleSGBD();
		
		// Create two relations with two tuples each, and with a common attribute
		SimpleDBRelation relation1 = sgbd.createRelation("RELONE", "attrA", "attrB");
		relation1.addTuple(new String[]{"a1", "b1"});
		relation1.addTuple(new String[]{"a2", "b2"});
		
		SimpleDBRelation relation2 = sgbd.createRelation("RELTWO", "attrC", "attrB");
		relation2.addTuple(new String[]{"c1", "b1"});
		relation2.addTuple(new String[]{"c2", "b1"});
		
		// Apply a join operation JOIN on these relation
		String resultRelName = tp2.computeJoin(sgbd, "RELONE", "RELTWO");
		
		// Check the correct relation description of the result: the relation description is the same as the one of the input relation
		assertArrayEquals(new String[]{"attrA", "attrB", "attrC"}, sgbd.getRelation(resultRelName).getRelationSchema().getSort());
		
		// Check that the result relation contains the correct tuples
		// These tuples can appear in any order
		List<String[]> expectedResultTuples = new ArrayList<>();
		expectedResultTuples.add(new String[]{"a1", "b1", "c1"});
		expectedResultTuples.add(new String[]{"a1", "b1", "c2"});
		
		if (! tuplesOfRelationAre(sgbd, resultRelName, expectedResultTuples)) {
			String failMessage = String.format("Wrong result. Expected: %s. Found %s",
					listToString(expectedResultTuples),
					listToString(collectAllTuplesOfARelation(sgbd, resultRelName)));
			fail(failMessage);
		}
	}
	
	
	
	// -----------------------------------------------------------------------------------
	// UTILITY METHODS
	// -----------------------------------------------------------------------------------	

	
	/** Checks whether the set of tuples of a relation are those enumerated as last parameter. */ 
	private boolean tuplesOfRelationAre (SimpleSGBD sgbd, String relName, String[] ... expTuples) {
		List<String[]> list = new ArrayList<>();
		list.addAll(Arrays.asList(expTuples));
		return tuplesOfRelationAre(sgbd, relName, list);
	}
	
	// Utility method
	/** Checks whether the set of tuples of a relation are those from the list given in parameter */
	private boolean tuplesOfRelationAre (SimpleSGBD sgbd, String relName, List<String[]> expTuples) {
		List<String[]> actualTuples = collectAllTuplesOfARelation(sgbd, relName);
		
		if (actualTuples.size() != expTuples.size())
			return false;
		
		for (String[] el: actualTuples)
			if (! listContains(expTuples, el))
				return false;
		
		for (String[] el: expTuples)
			if (! listContains(actualTuples, el))
					return false;
		
		return true;
	}
	
	
	// Utility method
	private List<String[]> collectAllTuplesOfARelation (SimpleSGBD sgbd, String relName) {
		List<String[]> tuples = new ArrayList<>();
		SimpleDBRelation relation = sgbd.getRelation(relName);
		relation.switchToReadMode();
		String[] rt;
		while ((rt = relation.nextTuple()) != null) {
			tuples.add(rt);
		}
		return tuples;
	}
	
	// Utility method
	private String listToString (List<String[]> list) {
		StringBuilder s = new StringBuilder();
		for (String[] el : list) {
			s.append(Arrays.toString(el) + ",");
		}
		return "[" + s.toString().substring(0, s.length()-1) + "]";
	}
	
	private boolean listContains (List<String[]> list, String[] tuple) {
		for (String[] el: list)
			if (Arrays.equals(el, tuple))
				return true;
		return false;
	}
}
