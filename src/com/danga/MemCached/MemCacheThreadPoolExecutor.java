/**
 * src/com/danga/MemCached/MemCacheThreadPoolExecutor.java
 *
 * @author $Author: $
 * @version $Revision: $ $Date: $
 * copyright (c) 2005 meetup, inc
 *
 * $Id: $
 */
package com.danga.MemCached;

import java.util.concurrent.*;
import java.util.*;
import org.apache.log4j.Logger;

public class MemCacheThreadPoolExecutor extends ThreadPoolExecutor {

	// logger
	private static Logger log =
		Logger.getLogger( MemCacheThreadPoolExecutor.class.getName() );

	private String poolName;
	private String host;
	private SockIOPool.SockIO socket;
	private boolean socketOnDemand;

	/** 
	 * 
	 * 
	 * @param corePoolSize 
	 * @param maximumPoolSize 
	 * @param keepAliveTime 
	 * @param unit 
	 * @param workQueue 
	 * @param pooName 
	 * @param host 
	 */
	public MemCacheThreadPoolExecutor( int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, String poolName, String host ) {

		super( corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue );

		if ( poolName == null
				|| host == null
				|| "".equals( poolName )
				|| "".equals( host ) )
			throw new IllegalArgumentException( "missing one of host: " + host + " or pool: " + poolName );

		this.poolName = poolName;
		this.host     = host;

		// get socket if we have a pool of only a single thread
		// else we will get socket each time
		if ( corePoolSize == maximumPoolSize
				&& corePoolSize == 1 ) {
			this.socket =
				SockIOPool.getInstance( poolName ).getConnection( host );

			this.socketOnDemand = false;
		}
		else {
			this.socketOnDemand = true;

		}
	}

	protected void beforeExecute( Thread t, Runnable r ) {

		// make sure we have a valid connected socket
		// if not, then get a new connection
		if ( socketOnDemand || this.socket == null || !this.socket.isConnected() ) {
			((CacheTask)r).setSocket( SockIOPool.getInstance( poolName ).getConnection( host ) );
		}
		else {
			((CacheTask)r).setSocket( this.socket );
		}

		super.beforeExecute( t, r );
	}

	protected void afterExecute( Runnable r, Throwable t ) {
		super.afterExecute( r, t );

		if ( socketOnDemand ) {
			// return to pool
			if ( ((CacheTask)r).getSocket() != null )
				((CacheTask)r).getSocket().close();
		}
		else {
			// freshen the socket so we have it around
			if ( this.socket != null )
				this.socket.touch();
			else
				this.socket = null;
		}
	}

	protected void terminated() {
		if ( this.socket != null && this.socket.isConnected() )
			this.socket.close();

		super.terminated();
	}
}
