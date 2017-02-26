package univlille.m1info.abd.tp5;

import java.util.ArrayList;
import java.util.List;

import univlille.m1info.abd.ra.JoinQuery;

public class JoinQueriesOptimizer {

	private List<JoinQuery[]> swappedQueries = null;
	private JoinQuery[] queries = null;
	
	public JoinQueriesOptimizer(JoinQuery ... queries) {
		this.queries = queries;
		swappedQueries = new ArrayList<>();
	}
	
	public void clear() {
		swappedQueries.clear();
	}
	
	public void computeSwap() {
		JoinQuery[] currentSwap = new JoinQuery[queries.length];
		JoinQuery[] previousSwap = new JoinQuery[queries.length];
		
		for(int i = 0; i < queries.length; i++)
			currentSwap[i] = queries[i];
		
		swappedQueries.add(currentSwap);
		previousSwap = currentSwap;
		
		int length = 0;
		
		for(int i = 0; i < queries.length; i++) {
			currentSwap = new JoinQuery[queries.length];
			currentSwap[0] = queries[i];
			length = 1;
			
			swappedQueries.add(currentSwap);
			previousSwap = currentSwap;
		}
	}
}
