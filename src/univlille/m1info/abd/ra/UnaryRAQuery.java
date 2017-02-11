package univlille.m1info.abd.ra;

/** An abstract class for the three unary operations of the relational algebra: projection, selection and rename.
 *  
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 1 févr. 2017
 */
public abstract class UnaryRAQuery implements RAQuery {

	private final RAQuery subQuery;
	
	public UnaryRAQuery(RAQuery subQuery) {
		this.subQuery = subQuery;
	}

	public RAQuery getSubQuery() {
		return subQuery;
	}
}
