package univlille.m1info.abd.tp4;

import java.util.ArrayList;
import java.util.List;

import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.ra.JoinQuery;
import univlille.m1info.abd.ra.ProjectionQuery;
import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RAQueryVisitor;
import univlille.m1info.abd.ra.RelationNameQuery;
import univlille.m1info.abd.ra.SelectionQuery;
import univlille.m1info.abd.ra.Visitor;

public class TP4 {

	public static void main(String[] args) {
		RAQueryVisitor visitor = new Visitor();
		setUpQueryTree(visitor);
	}

	private static void setUpQueryTree(RAQueryVisitor visitor) {
		List<RAQuery> queries = new ArrayList<>(), globalQueries = new ArrayList<>(), optimizedQueries = new ArrayList<>();

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
	}
}
