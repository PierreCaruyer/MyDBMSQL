package univlille.m1info.abd.tp4;

import univlille.m1info.abd.ra.DisplayVisitor;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RelationNameQuery;

public class TP4 {

	public static void main(String[] args) {
		RAQuery q = new RelationNameQuery("a");
		DisplayVisitor visitor = new DisplayVisitor();
		setUpQueryTree(visitor);
	}

	private static void setUpQueryTree(DisplayVisitor visitor) {
		/*List<RAQuery> queries = new ArrayList<>();

		

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
