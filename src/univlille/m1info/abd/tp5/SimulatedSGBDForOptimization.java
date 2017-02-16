package univlille.m1info.abd.tp5;

import java.util.HashMap;
import java.util.Map;

public class SimulatedSGBDForOptimization {
	
	private Map<String, Integer> nbtMap = new HashMap<>();
	
	public void addRelation (String relName, int nbTuples) {
		nbtMap.put(relName, nbTuples);
	}
	
	public int getNbTuplesForRelation (String relName) {
		Integer r = nbtMap.get(relName);
		if (r == null) {
			throw new IllegalArgumentException("No such relation : " + relName);
		}
		return r;
	}
}
