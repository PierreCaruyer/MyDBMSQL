package univlille.m1info.abd.phys;

/** The exception indicates that the SGBD does not have free in-memory buffer for loading a new page.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 23 févr. 2017
 */
public class NotEnoughMemoryException extends Exception {

	private static final long serialVersionUID = -6314007225203951778L;

	public NotEnoughMemoryException() {
		super();
	}
	
	@Override
	public String toString() {
		return "Not enough memory";
	}
}
