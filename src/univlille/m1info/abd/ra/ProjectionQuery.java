package univlille.m1info.abd.ra;

import java.util.Arrays;

/** A projection operation of the relational algebra. 
 * This is a unary operation and is parameterized by a projection criterion, that is a sequence of column ranks on which to project.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 19 févr. 2016
 */
public class ProjectionQuery extends UnaryRAQuery implements RAQuery {

	private String[] attributeNames;
	
	public ProjectionQuery(RAQuery subQuery, String ... projectedAttributesNames) {
		super(subQuery);
		if (projectedAttributesNames.length == 0)
			throw new IllegalArgumentException("Projection on 0 attributes not allowed");
		this.attributeNames = Arrays.copyOf(projectedAttributesNames, projectedAttributesNames.length);
	}
	
	public String[] getProjectedAttributesNames() {
		return Arrays.copyOf(attributeNames, attributeNames.length);
	}

	@Override
	public String toString() {
		return String.format("PROJECT%s(%s)", Arrays.toString(attributeNames), getSubQuery());
	}
	
	public String[] getAttributeNames() {
		return attributeNames;
	}
}
