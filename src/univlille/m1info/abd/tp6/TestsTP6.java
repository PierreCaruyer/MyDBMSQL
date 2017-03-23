package univlille.m1info.abd.tp6;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.schema.DefaultRelationSchema;
import univlille.m1info.abd.schema.RelationSchema;

public class TestsTP6 {	

	// NOTE: remplacer ParcoursTableParPage par votre classe du TP6 - 5.1 Q1
	
	@Test
	public void testParcoursTable () throws IOException, NotEnoughMemoryException {
		
		RelationSchema schema = new DefaultRelationSchema("REL", "ra", "rb");
		MyMemoryManager mem = new MyMemoryManager(5, 2);
		DefaultRelation rel = new DefaultRelation(schema, mem);
		
		ArrayList<String[]> tuples = new ArrayList<>();
		for (int i = 1; i <=9; i++) {
			tuples.add(new String[]{"a"+(i%3), "b"+i});	
		}
		
		rel.loadTuples(tuples);
		
		
		ParcoursTableParPages tableOp = new ParcoursTableParPages(rel, mem);
		int testNbTuples = 0;
		int testNbPages = 0;
		
		int pageNb;
		while ((pageNb = tableOp.nextPage()) != -1) {
			testNbPages++;
			Page page = mem.loadPage(pageNb, false);
			page.switchToReadMode();
			String[] tuple;
			while ((tuple = page.nextTuple()) != null) {
				System.out.println(Arrays.toString(tuple));
				testNbTuples++;
			}
			mem.releasePage(pageNb);
		}
		assertEquals(9, testNbTuples);
		assertEquals(4, testNbPages);
	}
	
	
	// Remplacer FiltreSelection par la classe du TP6 - Section 6 Q1 
	// Sachant qu'ici le filtrage est une simple sélection
	
	@Test
	public void testSelection1 () throws IOException, NotEnoughMemoryException {
				
		RelationSchema schema = new DefaultRelationSchema("REL", "ra", "rb");
		MyMemoryManager mem = new MyMemoryManager(100, 2);  
		DefaultRelation rel = new DefaultRelation(schema, mem);
		
		ArrayList<String[]> tuples = new ArrayList<>();
		for (int i = 1; i <=9; i++) {
			tuples.add(new String[]{"a"+(i%3), "b"+i});	
		}
		
		rel.loadTuples(tuples);		
		
		ParcoursTableParPages tableOp = new ParcoursTableParPages(rel, mem);
		FiltreSelection sel = new FiltreSelection(tableOp, "ra", "a1", mem);
		
		int pageNb;
		while ((pageNb = sel.nextPage()) != -1) {
			System.out.println(pageNb);
			Page page = mem.loadPage(pageNb, false);
			page.switchToReadMode();
			String[] tuple;
			while ((tuple = page.nextTuple()) != null) {
				System.out.println(Arrays.toString(tuple));
			}
			mem.releasePage(pageNb);
		}
		System.out.println("Number of operations : " + mem.getNumberOfDiskReadSinceLastReset());
		
		
		System.out.println("RESET");
		
		List<String[]> result = new ArrayList<>();
		sel.reset();
		while ((pageNb = sel.nextPage()) != -1) {
			System.out.println(pageNb);
			Page page = mem.loadPage(pageNb, false);
			page.switchToReadMode();
			String[] tuple;
			while ((tuple = page.nextTuple()) != null) {
				result.add(tuple);
				System.out.println(Arrays.toString(tuple));
			}
			mem.releasePage(pageNb);
		}
		assertEquals(3, result.size());
		
	}
	
	// Remplacer JoinWithPages par la classe de TP6 - Section 7 Q2
	
	
	@Test
	public void testJoin() throws IOException, NotEnoughMemoryException {
		
		RelationSchema schema1 = new DefaultRelationSchema("RELONE", "ra", "rb");
		RelationSchema schema2 = new DefaultRelationSchema("RELTWO", "ra", "rc");
		MyMemoryManager mem = new MyMemoryManager(100, 2);
		
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
		
		ParcoursTableParPages tableOpLeft = new ParcoursTableParPages(rel1, mem);
		ParcoursTableParPages tableOpRight = new ParcoursTableParPages(rel2, mem);
		
		List<String[]> result = new ArrayList<>();
		
		JoinWithPages join = new JoinWithPages(tableOpLeft, tableOpRight, mem);
		int pageNb;
		while ((pageNb = join.nextPage()) != -1) {
			Page page = mem.loadPage(pageNb, false);
			page.switchToReadMode();
			String[] tuple;
			while ((tuple = page.nextTuple()) != null) {
				System.out.println(Arrays.toString(tuple));
				result.add(tuple);
			}
			mem.releasePage(pageNb);
		}

		assertEquals(27, result.size());
		
		System.out.println("Number of operations: " + mem.getNumberOfDiskReadSinceLastReset());
	}
	
	
	
}
