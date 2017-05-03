package univlille.m1info.abd.tp7;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.phys.PhysicalOperator;
import univlille.m1info.abd.tp6.DisposalRelations;

public class TestTP7 {
	
	private DisposalRelations relationsAtDisposal;
	private SchemawithMemory memSchema;
	private String testedRelName = "RELLONGR";
	private String testedColName = "attrA";
	
	@Before
	public void setUp() {
		memSchema = new SchemawithMemory();
		relationsAtDisposal = new DisposalRelations(memSchema);
	}
	
	@Test
	public void testMultiplePageIndex() {
		PhysicalOperator operator = relationsAtDisposal.getLongRightTable();
		testedRelName = operator.resultSchema().getName();
		assertTrue(moreThanOneAddress(memSchema.getIndex(testedRelName, testedColName).getListofAddresses(new String[] { "a5", "b1", "c3" })));
	}

	private boolean moreThanOneAddress(List<Integer> list) {
		int count = list.size();
		return count > 1;
	}
}
