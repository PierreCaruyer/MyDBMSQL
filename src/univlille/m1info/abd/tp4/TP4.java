package univlille.m1info.abd.tp4;

import univlille.m1info.abd.ra.DisplayVisitor;

public class TP4 {

	public static void main(String[] args) {
		DisplayVisitor visitor = new DisplayVisitor();
		setUpQueryTree(visitor);
	}

	private static void setUpQueryTree(DisplayVisitor visitor) {
		/*List<RAQuery> queries = new ArrayList<>();

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
		System.out.println(selection);*/
	}
}
