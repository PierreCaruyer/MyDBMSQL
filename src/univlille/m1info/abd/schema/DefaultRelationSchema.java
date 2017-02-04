
package univlille.m1info.abd.schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** A default implementation of {@link RelationSchema}.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 26 janv. 2017
 */
public class DefaultRelationSchema implements RelationSchema {
	
	private final String name;
	private final String[] attributeNames;
	private static final String ATTR_NAMES_REGEX = "[a-zA-Z][a-zA-Z_]+";
	
	public DefaultRelationSchema (String name, String ... attributeNames) {
		
		Set<String> usedNames = new HashSet<>();
		checkCorrectRelationName(name);
		usedNames.add(name);
		this.name = name;

		if (attributeNames.length == 0)
			throw new IllegalArgumentException("A relation description should have at least one attribute");
		
		this.attributeNames = new String[attributeNames.length];
		for (int i = 0; i < attributeNames.length; i++) {
			String currentAttribute = attributeNames[i];
			checkCorrectAttributeName(currentAttribute, usedNames);
			this.attributeNames[i] = currentAttribute;
			usedNames.add(currentAttribute);
		}
	}
	
	private void checkCorrectAttributeName (String name, Set<String> knownNames) {
		if (name == null) 
			throw new IllegalArgumentException("Null relation or attribute name not allowed.");
		if (knownNames.contains(name)) 
			throw new IllegalArgumentException("Repeated attribute or relation name not allowed: " + name);
		if (! name.matches(ATTR_NAMES_REGEX)) 
			throw new IllegalArgumentException("Relation or attribute name should satisfy the regex " + ATTR_NAMES_REGEX);
	}
	
	protected void checkCorrectRelationName (String name) {
		if (name == null) 
			throw new IllegalArgumentException("Null relation or attribute name not allowed.");
		if (! name.matches(ATTR_NAMES_REGEX)) 
			throw new IllegalArgumentException("Relation or attribute name should satisfy the regex " + ATTR_NAMES_REGEX);
	}
	
	@Override
	public String[] getSort() {
		return Arrays.copyOf(attributeNames, attributeNames.length);
	}
	
	@Override
	public String getName() {
		return name;
	}

	private int getRankOfAttribute(String attributeName) {
		for (int i = 0; i < attributeNames.length; i++) {
			if (attributeNames[i].equals(attributeName))
				return i;
		}
		return -1;
	}

	@Override
	public String toString() {
		String attr = Arrays.toString(attributeNames);
		attr = attr.substring(1, attr.length()-1);
		return String.format("%s(%s)", name, attr);
	}
	
	@Override
	public String getAttributeValue (String[] tuple, String attributeName) {
		int rank = getRankOfAttribute(attributeName);
		if (rank == -1)
			return null;
		return tuple[rank];
	}
	
	@Override
	public void setAttributeValue (String newValue, String[] tuple, String attributeName) {
		tuple[getRankOfAttribute(attributeName)] = newValue;
	}
	
	@Override
	public String[] newEmptyTuple () {
		return new String[attributeNames.length];	
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(attributeNames);
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DefaultRelationSchema other = (DefaultRelationSchema) obj;
		if (!Arrays.equals(attributeNames, other.attributeNames))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
}
