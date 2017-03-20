package univlille.m1info.abd.phys;

import univlille.m1info.abd.memorydb.NotEnoughMemoryException;
import univlille.m1info.abd.memorydb.Page;

/** Simulates main memory with limited number of pages that are available for being used by an DBMS.
 * The number of pages is given by SGBD.MEMORY_SIZE
 * 
 * Allows to load a physical page within an in-memory buffer, and to write a buffer to a physical page.
 * 
 * A physical page is identified by its page number.
 * 
 * Allows also to monitor the number of disk operations.
 * 
 * @author Iovka Boneva and Pierre Bourhis
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 20 janv. 2017
 */
public interface MemoryManager {
	
	/** Loads a physical page from disk for a relation given as input and the arity of the tuples of the relation and returns the symbolic representation of this page  
	 * 
	 * @param pageAddress The address of the page to be loaded 
	 * @return 
	 */
	public Page loadPage (int pageAddress) throws NotEnoughMemoryException ;

	
	/**
	 *Create a new empty Page in the Disk and return the address of this new page 
	 *
	 * @return
	 * @throws NotEnoughMemoryException
	 */
	

	public Page NewPage (int Arity) throws NotEnoughMemoryException ;
	
	/**
	 * Update the Page at Address as follows : load the new page Datapage in the memory associated with the address Address.
	 * This overwrite the previous value of the page of its address
	 * Be careful this new value is not soted in the Disk until the page is release
	 * @param Datapage Ask to put in memory a page 
	 * 
	 * Be careful, the page is not stored in the disk
	 */
	public void PutinMemory (Page Datapage, int Address) throws NotEnoughMemoryException;
	
	
	
	/** Indicates that the page is no more used, thus the memory slot becomes available again.
	 * If this page is a page added to the memory manager and the value persistant is equal to true then this page is saved to the disk
	 * 
	 * @param pageAddress, if the address is not in memory nothing is done
	 * @param persistant, if the address is a temporary one and persistant is equal to True then, 
	 * this page is stored in the temporary memory
	 */
	
	public void releasePage (int pageAddress, boolean persistant);
	

	/**
	 * Change the value of the previous page address (PrevAdd) in the page of Address Add
	 * This is possible only for new pages added in Disk
	 * @param Add the
	 * @param PrevAdd
	 */

	//public void setPrevAdd (int Add, int PrevAdd);
	
	
	/**
	 * Change the value of the next page address (NextAdd) in the page of Address Add
	 * This is possible only for new pages added in Disk
	 * @param Add
	 * @param NextAdd
	 */

	//public void setNextAdd (int Add, int NextAdd);
	
	
	
	/**
	 * Reset the reading operations done on the disk 
	 *
	 */
	public void resetDiskOperationsCount();
	
	/**
	 * 
	 * @return the number of load of pages done from the disk
	 */
	
	public int getNumberOfDiskReadSinceLastReset();
	
	/**
	 * 
	 * @return the number of read asked to the Memory Manager 
	 */
	
	public int getNumberOfReadSinceLastReset();
	
	/**
	 * 
	 * @return the number of write asked to the last resest
	 */
	
	public int getNumberofWriteDiskSinceLastReset();
	
	
	/**
	 * 
	 * @return Total of read operations
	 */
	
	public int getTotalNumberOfDiskOperations();
	

}