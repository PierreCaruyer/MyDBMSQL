package univlille.m1info.abd.tp4;

import univlille.m1info.abd.ra.RAQuery;
import univlille.m1info.abd.ra.RAQueryVisitor;

public class TP4 {

	public void goThroughQueryTree(RAQuery topQuery, RAQueryVisitor visitor) {
		topQuery.accept(visitor);
	}
}
