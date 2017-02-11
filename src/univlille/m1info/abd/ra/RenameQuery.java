package univlille.m1info.abd.ra;

/** A rename operation of the relational algebra.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 25 janv. 2017
 */
public class RenameQuery extends UnaryRAQuery implements RAQuery {

	private final String oldAttrName;
	private final String newAttrName;
	
	public RenameQuery(RAQuery subQuery, String oldAttName, String newAttrName) {
		super(subQuery);
		this.oldAttrName = oldAttName;
		this.newAttrName = newAttrName;
	}

	public String getOldAttrName() {
		return oldAttrName;
	}

	public String getNewAttrName() {
		return newAttrName;
	}
	
	@Override
	public String toString() {
		return String.format("RENAME[%s/%s] ()", oldAttrName, newAttrName, getSubQuery());
	}

	@Override
	public void accept(RAQueryVisitor v) {
		v.visit(this);
	}
}
