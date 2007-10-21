/**
 * src/com/danga/MemCached/LineInputStream.java
 *
 * @author $Author: $
 * @version $Revision: $ $Date: $
 * copyright (c) 2006 meetup, inc
 *
 * $Id: $
 */
package com.danga.MemCached;

import java.io.IOException;

public interface LineInputStream {
    
	/**
	 * Read everything up to the next end-of-line.  Does
	 * not include the end of line, though it is consumed
	 * from the input.
	 * @return  All next up to the next end of line.
	 */
	public String readLine() throws IOException;
	
	/**
	 * Read everything up to and including the end of line.
	 */
	public void clearEOL() throws IOException;
	
	/**
	 * Read some bytes.
	 * @param buf   The buffer into which read. 
	 * @return      The number of bytes actually read, or -1 if none could be read.
	 */
	public int read( byte[] buf ) throws IOException;
}
