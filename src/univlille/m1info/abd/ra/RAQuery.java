package univlille.m1info.abd.ra;

import univlille.m1info.abd.schema.RelationSchema;

/** The top-level interface representing a query expressed in the relational algebra.
 * The different classes implementing this interface allow to encode an expression of the relational algebra. 
 * An expression of the relational algebra has a tree structure which nodes are {@link RAQuery}s, and every node has
 * as many children as the arity of the corresponding operation (i.e. a join node has two children, a projection or selection node has
 * one child, and a table node is a leaf in the tree).
 * 
 * The type hierarchy of {@link RAQuery} and all related classes are used to describe the 
 * structure of query in the relational algebra, but <b>do not allow</b> to evaluate the query. 
 * For evaluating a query one must implement physical operators. 
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 19 févr. 2016
 */
public interface RAQuery {
	
	public String[] nextTuple();
	
	public void reset();
	
	public RelationSchema resultSchema();
}
