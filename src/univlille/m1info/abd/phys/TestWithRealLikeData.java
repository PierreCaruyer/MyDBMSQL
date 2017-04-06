package univlille.m1info.abd.phys;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.schema.DefaultRelationSchema;
import univlille.m1info.abd.tp6.TP6;
import univlille.m1info.abd.tp6.TestTP6;

public class TestWithRealLikeData {
	
	private TP6 tp6;
	private MemoryManager mem;
	
	@Before
	public void setUp() {
		tp6 = new TP6(TestTP6.PAGE_SIZE, TestTP6.ATTRIBUTE_SIZE);
		mem = tp6.getMemoryManager();
	}

	private List<String[]> loadDataFromCSVFile (String fileName, char separator) throws IOException {
		List<String> lines = Files.readAllLines(Paths.get(fileName));
		String sep = ""+separator;
		
		List<String[]> result = new ArrayList<>();
		for (String line: lines) {
			String[] tuple = line.split(sep);
			for (int i = 0; i < tuple.length; i++) {
				tuple[i] = tuple[i].trim();
				tuple[i] = tuple[i].replace("\"","");
			}
			result.add(tuple);
		}
		return result;
	}

	/** The first line of the file is treated as attribute names. 
	 * @throws IOException */
	private DefaultRelation createRelationFromCSVFile (String fileName, char separator, String relName) throws IOException {
		List<String[]> tuples = loadDataFromCSVFile(fileName, separator);
		DefaultRelationSchema schema = new DefaultRelationSchema(relName, tuples.get(0));
		DefaultRelation rel = new DefaultRelation(schema, SchemawithMemory.mem);
		tuples.remove(0);
		rel.loadTuples(tuples);
		return rel;
	}


	@Test
	public void testExample () throws IOException, NotEnoughMemoryException {
		DefaultRelation relFilm = createRelationFromCSVFile("/tmp/FILMS.csv", ',', "FILMS");
		//DefaultRelation relPresident = createRelationFromCSVFile("/tmp/USPRESIDENT.csv", ',' , "PRESIDENTS");
		
		SequentialAccessOnARelationOperator tableOp = new SequentialAccessOnARelationOperator(relFilm, mem);
		SelectionOperator sel = new SelectionOperator(tableOp, "fannee", "1990", ComparisonOperator.EQUAL, mem);
		
		int pageNb;
		while ((pageNb = sel.nextPage()) != -1) {
			Page page = SchemawithMemory.mem.loadPage(pageNb);
			page.switchToReadMode();
			String[] tuple;
			while ((tuple = page.nextTuple()) != null) {
				System.out.println(Arrays.toString(tuple));
			}
			SchemawithMemory.mem.releasePage(pageNb, true);
		}
		
	}
	
}