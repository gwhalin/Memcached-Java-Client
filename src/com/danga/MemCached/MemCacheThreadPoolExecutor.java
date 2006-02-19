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

public class MemCacheThreadPoolExecutor extends ThreadPoolExecutor {

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
		super.beforeExecute( t, r );

		// make sure we have a valid connected socket
		// if not, then get a new connection
		if ( socketOnDemand
				|| socket == null
				|| !socket.isConnected() )
			socket = SockIOPool.getInstance( poolName ).getConnection( host );

		((CacheTask)r).setSocket( socket );

	}

	protected void afterExecute( Runnable r, Throwable t ) {
		super.afterExecute( r, t );

		socket = ((CacheTask)r).getSocket();
		if ( socketOnDemand ) {
			// return to pool
			if ( socket != null )
				socket.close();
		}
		else {
			// freshen the socket so we have it around
			if ( socket != null )
				socket.touch();
			else
				this.socket = null;
		}
	}
}
