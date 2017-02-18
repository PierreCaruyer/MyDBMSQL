package univlille.m1info.abd.tp4;

import java.util.ArrayList;
import java.util.List;

import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.ra.JoinQuery;
import univlille.m1info.abd.ra.ProjectionQuery;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RelationNameQuery;
import univlille.m1info.abd.ra.SelectionQuery;
import univlille.m1info.abd.ra.UnaryRAQuery;
import univlille.m1info.abd.ra.Visitor;

public class TP4 {

	public static void main(String[] args) {
		Visitor visitor = new Visitor();
		setUpQueryTree(visitor);
	}

	private static void setUpQueryTree(Visitor visitor) {
		List<RAQuery> queries = new ArrayList<>();

		RelationNameQuery table1 = new RelationNameQuery("R");
		RelationNameQuery table2 = new RelationNameQuery("S");
		JoinQuery join = new JoinQuery(table1, table2);
		SelectionQuery selec = new SelectionQuery(join, "attrB", ComparisonOperator.EQUAL, "5");
		ProjectionQuery proj = new ProjectionQuery(selec, new String[]{"attrB"});

		proj.accept(visitor);
		visitor.switchToReadMode();
		for(RAQuery q = visitor.nextQuery(); q != null; q = visitor.nextQuery()) {
			queries.add(q);
			System.out.println(queries.get(queries.size() - 1));
		}
		
		SelectionQuery selection = visitor.nextSelection();
		System.out.println(selection);
	}
	
	/**
	 * Starts at some node of the tree and skips all selection queries from there
	 * @param currentEntryPoint
	 * @return the first query that is not a selection
	 */
	public static RAQuery skipSelectionsFrom(RAQuery entryPoint) {
		RAQuery currentNode = entryPoint;
		UnaryRAQuery currentUnary = null;
		
		if(currentNode instanceof UnaryRAQuery) {
			currentUnary = (UnaryRAQuery)currentNode;
			while(currentUnary instanceof SelectionQuery) {
				currentNode = currentUnary.getSubQuery();
				if(currentNode instanceof UnaryRAQuery)
					currentUnary = (UnaryRAQuery)currentNode;
				else
					break;
			}
		}
		
		return currentNode;
	}
	
	/**
	 * Pushes selection queries down in the query tree
	 * @param root
	 * @return root of the newly optimized query tree
	 */
	public static RAQuery optimizeTree(RAQuery root) {
		RAQuery optimRoot = null, currentNode = null, currentOptimNode = null;

		currentNode = skipSelectionsFrom(root);
		currentOptimNode = QueryFactory.copyQuery(currentNode);
		if(currentOptimNode instanceof JoinQuery) {
			
		}
		/*if(currentNode == root) { //skipSelection returned root therefore root isn't an instance of UnaryRAQuery and should be treated independantly
			
		}
		else { 
			
		}*/
		
		return optimRoot;
	}
}
