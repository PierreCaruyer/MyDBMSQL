package univlille.m1info.abd.phys;

/** This object gives a representation of a page allowing to manipulate directly the tuples.
 * 
 * It is used to decode an array of Byte obtained from the MemoryManager.
 * @author Pierre Bourhis
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 28 fev. 2017
 */

public interface Page {


	public int getAddressnextPage ();
	
	public int getAddressPage();
	
	public int getAddresspreviousPage ();
	
	
	/**
	 * 
	 * @return the next tuple using the iterator
	 */
	public String[] nextTuple ();
	
	/**
	 * 
	 * @return a vector of bytes corresponding to the translation of the page in bytes.
	 */
	
	//public byte [] translateToByte();
	
	/**
	 * 
	 * @return the number of tuples stored in the page
	 */
	
	public int getNumberofTuple();
	
	
	/**
	 * Initialize the read of the tuples
	 */
	public void switchToReadMode(); 
	
	
	
	/**
	 * 
	 * @param Tuple
	 * @return Add a tuple to this page
	 * This is useful only when constructing in a new page
	 */
	
	
	
	public boolean AddTuple (String[] Tuple);
	
	/**
	 * This links the current page to a previous page by its address (PrevAdd) where it is stored in the Disk
	 * This is useful only when constructing a new page
	 * @param PrevAdd
	 */
	
	
	public void SetPrevAdd (int PrevAdd);
	
	
	/**
	 * This memorizes the address where the current page is stored in the Disk
	 * @param Add
	 */
	
	public void SetAdd (int Add);
	
	/**
	 * 
	 * This links the current page to a next page by its address (NextAdd) where it is stored in the Disk
	 * This is useful only when constructing a new page
	 * @param NextAdd
	 */
	
	
	
	public void SetNextAdd (int NextAdd);
	
}
