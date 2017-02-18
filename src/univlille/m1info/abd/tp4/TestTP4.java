package univlille.m1info.abd.tp4;

import org.junit.Test;

import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.ra.JoinQuery;
import univlille.m1info.abd.ra.OptimizerVisitor;
import univlille.m1info.abd.ra.ProjectionQuery;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RelationNameQuery;
import univlille.m1info.abd.ra.SelectionQuery;
import univlille.m1info.abd.simplebd.SimpleDBRelation;
import univlille.m1info.abd.simplebd.SimpleSGBD;

/**
 * Custom test class in order to test 
 */
public class TestTP4 {
	
	public RAQuery getRootQuery_non_optimized_tree() {
		RelationNameQuery table1 = new RelationNameQuery("R");
		RelationNameQuery table2 = new RelationNameQuery("S");
		JoinQuery join = new JoinQuery(table1, table2);
		SelectionQuery selec = new SelectionQuery(join, "attrC", ComparisonOperator.EQUAL, "5");
		ProjectionQuery proj = new ProjectionQuery(selec, new String[]{"attrB"});
		return proj;
	}
	
	public RAQuery getRootQuery_optimized_tree() {
		RelationNameQuery table1 = new RelationNameQuery("R");
		RelationNameQuery table2 = new RelationNameQuery("S");
		SelectionQuery selec = new SelectionQuery(table2, "attrC", ComparisonOperator.EQUAL, "5");
		JoinQuery join = new JoinQuery(table1, selec);
		ProjectionQuery proj = new ProjectionQuery(join, new String[]{"attrB"});
		return proj;
	}
	
	public RAQuery getRootQuery_selection_as_top_query() {
		RelationNameQuery table1 = new RelationNameQuery("R");
		RelationNameQuery table2 = new RelationNameQuery("S");
		JoinQuery join = new JoinQuery(table1, table2);
		ProjectionQuery proj = new ProjectionQuery(join, new String[]{"attrB"});
		SelectionQuery selec = new SelectionQuery(proj, "attrC", ComparisonOperator.EQUAL, "5");
		return selec;
	}
	
	@Test
	public void testOptimizerVisitorNonOptimizedTree() {
		SimpleSGBD sgbd = new SimpleSGBD();
		OptimizerVisitor optimVisitor = new OptimizerVisitor();
		
		SimpleDBRelation r = sgbd.createRelation("R", "attrA", "attrB");
		r.addTuple(new String[]{"10", "9"});
		r.addTuple(new String[]{"8", "7"});
		
		SimpleDBRelation s = sgbd.createRelation("S", "attrA", "attrC", "attrD");
		s.addTuple(new String[]{"1", "2", "3"});
		s.addTuple(new String[]{"4", "5", "6"});
		
		RAQuery root = getRootQuery_non_optimized_tree();
		
		root.accept(optimVisitor);
		
		RAQuery optimizedRoot = optimVisitor.getOptimizedRoot();
	}
	
	@Test
	public void testOptimizerVisitorOnOptimizedTree() {
		SimpleSGBD sgbd = new SimpleSGBD();
		OptimizerVisitor optimVisitor = new OptimizerVisitor();
		
		SimpleDBRelation r = sgbd.createRelation("R", "attrA", "attrB");
		r.addTuple(new String[]{"10", "9"});
		r.addTuple(new String[]{"8", "7"});
		
		SimpleDBRelation s = sgbd.createRelation("S", "attrA", "attrC", "attrD");
		s.addTuple(new String[]{"1", "2", "3"});
		s.addTuple(new String[]{"4", "5", "6"});
		
		RAQuery root = getRootQuery_non_optimized_tree();
		
		root.accept(optimVisitor);
		
		RAQuery optimizedRoot = optimVisitor.getOptimizedRoot();
	}
	
	@Test
	public void testOptimizerVisitorOnSelectionQueryAsRootTree() {
		SimpleSGBD sgbd = new SimpleSGBD();
		OptimizerVisitor optimVisitor = new OptimizerVisitor();
		
		SimpleDBRelation r = sgbd.createRelation("R", "attrA", "attrB");
		r.addTuple(new String[]{"10", "9"});
		r.addTuple(new String[]{"8", "7"});
		
		SimpleDBRelation s = sgbd.createRelation("S", "attrA", "attrC", "attrD");
		s.addTuple(new String[]{"1", "2", "3"});
		s.addTuple(new String[]{"4", "5", "6"});
		
		RAQuery root = getRootQuery_non_optimized_tree();
		
		root.accept(optimVisitor);
		
		RAQuery optimizedRoot = optimVisitor.getOptimizedRoot();
	}
}
