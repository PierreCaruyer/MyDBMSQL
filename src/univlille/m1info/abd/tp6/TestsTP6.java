package univlille.m1info.abd.tp6;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.phys.JoinOperator;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.phys.SelectionOperator;
import univlille.m1info.abd.phys.SequentialAccessOnARelationOperator;
import univlille.m1info.abd.phys.SimpleMemoryManager;
import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.schema.DefaultRelationSchema;
import univlille.m1info.abd.schema.RelationSchema;

public class TestsTP6 {	

	// NOTE: remplacer ParcoursTableParPage par votre classe du TP6 - 5.1 Q1
	
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
