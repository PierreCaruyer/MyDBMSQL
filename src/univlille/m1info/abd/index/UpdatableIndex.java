package univlille.m1info.abd.index;

import java.util.List;

import univlille.m1info.abd.memorydb.SchemawithMemory;

public class UpdatableIndex extends DefaultIndex implements Index {

	public UpdatableIndex(String relName, String attribute, SchemawithMemory sgbd) {
		super(relName, attribute, sgbd);
	}

	public void updateIndex(String key, List<Integer> updatedValues) {
		index.remove(key);
		index.put(key, updatedValues);
	}
}
