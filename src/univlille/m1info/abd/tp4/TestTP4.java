package univlille.m1info.abd.tp4;

import org.junit.Test;
import static org.junit.Assert.*;

import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.ra.JoinQuery;
import univlille.m1info.abd.ra.OptimizerVisitor;
import univlille.m1info.abd.ra.ProjectionQuery;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RelationNameQuery;
import univlille.m1info.abd.ra.RenameQuery;
import univlille.m1info.abd.ra.SelectionQuery;
import univlille.m1info.abd.simplebd.SimpleDBRelation;
import univlille.m1info.abd.simplebd.SimpleSGBD;

/**
 * Custom test class in order to test 
 */
public class TestTP4 {
	
	public RAQuery getRootQuery_non_optimized_tree() {
		RelationNameQuery table1 = new RelationNameQuery("RELONE");
		RelationNameQuery table2 = new RelationNameQuery("RELTWO");
		JoinQuery join = new JoinQuery(table1, table2);
		SelectionQuery selec = new SelectionQuery(join, "attrC", ComparisonOperator.EQUAL, "5");
		ProjectionQuery proj = new ProjectionQuery(selec, new String[]{"attrB"});
		return proj;
	}
	
	public RAQuery getRootQuery_optimized_tree() {
		RelationNameQuery table1 = new RelationNameQuery("RELONE");
		RelationNameQuery table2 = new RelationNameQuery("RELTWO");
		SelectionQuery selec = new SelectionQuery(table2, "attrC", ComparisonOperator.EQUAL, "5");
		JoinQuery join = new JoinQuery(table1, selec);
		ProjectionQuery proj = new ProjectionQuery(join, new String[]{"attrB"});
		return proj;
	}
	
	public RAQuery getRootQuery_selection_as_top_query() {
		RelationNameQuery table1 = new RelationNameQuery("RELONE");
		RelationNameQuery table2 = new RelationNameQuery("RELTWO");
		JoinQuery join = new JoinQuery(table1, table2);
		ProjectionQuery proj = new ProjectionQuery(join, new String[]{"attrB"});
		SelectionQuery selec = new SelectionQuery(proj, "attrC", ComparisonOperator.EQUAL, "5");
		return selec;
	}
	
	@Test
	public void testOptimizerVisitorNonOptimizedTree() {
		SimpleSGBD sgbd = new SimpleSGBD();
		OptimizerVisitor optimVisitor = new OptimizerVisitor();
		
		SimpleDBRelation r = sgbd.createRelation("RELONE", "attrA", "attrB");
		r.addTuple(new String[]{"10", "9"});
		r.addTuple(new String[]{"8", "7"});
		
		SimpleDBRelation s = sgbd.createRelation("RELTWO", "attrA", "attrC", "attrD");
		s.addTuple(new String[]{"1", "2", "3"});
		s.addTuple(new String[]{"4", "5", "6"});
		
		RAQuery root = getRootQuery_non_optimized_tree();
		
		root.accept(optimVisitor);
		
		RAQuery optimizedRoot = optimVisitor.getOptimizedRoot();
		
		assertTrue(assertTreeStructureEquals(getRootQuery_optimized_tree(), optimizedRoot));
	}
	
	@Test
	public void testOptimizerVisitorOnOptimizedTree() {
		SimpleSGBD sgbd = new SimpleSGBD();
		OptimizerVisitor optimVisitor = new OptimizerVisitor();
		
		SimpleDBRelation r = sgbd.createRelation("RELONE", "attrA", "attrB");
		r.addTuple(new String[]{"10", "9"});
		r.addTuple(new String[]{"8", "7"});
		
		SimpleDBRelation s = sgbd.createRelation("RELTWO", "attrA", "attrC", "attrD");
		s.addTuple(new String[]{"1", "2", "3"});
		s.addTuple(new String[]{"4", "5", "6"});
		
		RAQuery root = getRootQuery_non_optimized_tree();
		
		root.accept(optimVisitor);
		
		RAQuery optimizedRoot = optimVisitor.getOptimizedRoot();
		
		assertTrue(assertTreeStructureEquals(getRootQuery_optimized_tree(), optimizedRoot));
	}
	
	@Test
	public void testOptimizerVisitorOnSelectionQueryAsRootTree() {
		SimpleSGBD sgbd = new SimpleSGBD();
		OptimizerVisitor optimVisitor = new OptimizerVisitor();
		
		SimpleDBRelation r = sgbd.createRelation("RELONE", "attrA", "attrB");
		r.addTuple(new String[]{"10", "9"});
		r.addTuple(new String[]{"8", "7"});
		
		SimpleDBRelation s = sgbd.createRelation("RELTWO", "attrA", "attrC", "attrD");
		s.addTuple(new String[]{"1", "2", "3"});
		s.addTuple(new String[]{"4", "5", "6"});
		
		RAQuery root = getRootQuery_non_optimized_tree();
		
		root.accept(optimVisitor);
		
		RAQuery optimizedRoot = optimVisitor.getOptimizedRoot();
		
		assertTrue(assertTreeStructureEquals(getRootQuery_optimized_tree(), optimizedRoot));
	}
	
	private boolean assertTreeStructureEquals(RAQuery expected, RAQuery actual) {
		RAQuery currentExpected = expected, currentActual = actual;
		Object currentExpectedClass = currentExpected.getClass();
		if(currentExpectedClass.equals(currentActual.getClass())) {
			if(currentExpectedClass.equals(ProjectionQuery.class)) {
				ProjectionQuery expectedProjection = (ProjectionQuery)currentExpected, actualProjection = (ProjectionQuery)currentActual;
				if(assertProjectionQueryEquals(expectedProjection, actualProjection))
					return assertTreeStructureEquals(expectedProjection.getSubQuery(), actualProjection.getSubQuery());
			}
			else if(currentExpectedClass.equals(SelectionQuery.class)) {
				SelectionQuery expectedSelection = (SelectionQuery)expected, actualSelection = (SelectionQuery)currentActual;
				if(assertSelectionQueryEquals(expectedSelection, actualSelection))
					return assertTreeStructureEquals(expectedSelection.getSubQuery(), actualSelection.getSubQuery());
			}
			else if(currentExpectedClass.equals(JoinQuery.class)) {
				JoinQuery expectedJoin = (JoinQuery)expected, actualJoin = (JoinQuery)actual;
				if(assertTreeStructureEquals(expectedJoin.getLeftSubQuery(), actualJoin.getLeftSubQuery()))
					return assertTreeStructureEquals(expectedJoin.getRightSubQuery(), actualJoin.getRightSubQuery());
			}
			else if(currentExpectedClass.equals(RenameQuery.class)) {
				RenameQuery expectedRename = (RenameQuery)expected, actualRename = (RenameQuery)currentActual;
				if(assertRenameQueryEquals(expectedRename, actualRename))
					return assertTreeStructureEquals(expectedRename.getSubQuery(), actualRename.getSubQuery());
			}
			else { //RelationNameQuery
				RelationNameQuery expectedRelationName = (RelationNameQuery)expected, actualRelationName = (RelationNameQuery)currentActual;
				return (expectedRelationName.getRelationName() == actualRelationName.getRelationName());
			}
		}
		
		return false;
	}
	
	private boolean assertProjectionQueryEquals(ProjectionQuery expected, ProjectionQuery actual) {
		return assertStringArrayEquals(expected.getProjectedAttributesNames(), actual.getProjectedAttributesNames());
	}
	
	private boolean assertSelectionQueryEquals(SelectionQuery expected, SelectionQuery actual) {
		return (expected.getAttributeName().equals(actual.getAttributeName()) &&
				expected.getConstantValue().equals(actual.getConstantValue()) &&
				expected.getComparisonOperator().equals(actual.getComparisonOperator()));
	}
	
	private boolean assertRenameQueryEquals(RenameQuery expected, RenameQuery actual) {
		return (expected.getOldAttrName().equals(actual.getOldAttrName()) &&
				expected.getNewAttrName().equals(actual.getNewAttrName()));
	}

	
	private boolean assertStringArrayEquals(String[] expected, String[] actual) {
		if(expected.length != actual.length)
			return false;
		
		for(int i = 0; i < expected.length; i++)
			if(!expected[i].equals(actual[i]))
				return false;
		
		return true;
	}
}
