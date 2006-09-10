/**
 * src/com/danga/MemCached/CacheTask.java
 *
 * @author $Author: $
 * @version $Revision: $ $Date: $
 * copyright (c) 2005 meetup, inc
 *
 * $Id: $
 */
package com.danga.MemCached;

import java.io.IOException;
import org.apache.log4j.Logger;

public class CacheTask implements Runnable {

	// logger
	private static Logger log =
		Logger.getLogger( CacheTask.class.getName() );

	// this should get set by the threadpool
	// before it executes the task
	private SockIOPool.SockIO socket;

	private String cmd;
	private byte[] val;

	public CacheTask( String cmd, byte[] val ) {
		this.cmd  = cmd;
		this.val  = val;
	}

	public String getCmd() {
		return this.cmd;
	}

	public void setSocket( SockIOPool.SockIO socket ) {
		this.socket = socket;
	}

	public SockIOPool.SockIO getSocket() {
		return this.socket;
	}

	public void run() {
		try {
			socket.write( cmd.getBytes() );
			if ( val != null ) {
				socket.write( val );
				socket.write( "\r\n".getBytes() );
			}
			socket.flush();
		}
		catch ( IOException ex ) {
			log.error( "++++ exception thrown while writing bytes to server" );
			log.error( ex.getMessage(), ex );

			try {
				socket.trueClose();
			}
			catch ( IOException ioe ) {
				log.error( "++++ failed to close socket : " + socket.toString() );
			}

			socket = null;
		}
	}
}
