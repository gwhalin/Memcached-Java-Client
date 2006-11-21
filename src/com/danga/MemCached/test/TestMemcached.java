/**
 *  * Test case to test memcached Java client failover when server list includes a server that is down
 *   */
package com.danga.MemCached.test;

import com.danga.MemCached.*;
import org.apache.log4j.*;

public class TestMemcached  {  
	public static void main(String[] args) {
		      // memcached should be running on port 11211 but NOT on 11212

		BasicConfigurator.configure();
		String[] servers = { "192.168.1.1:1624", "192.168.1.1:1625" };
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers( servers );
		pool.setFailover( true );
		pool.setInitConn( 10 ); 
		pool.setMinConn( 5 );
		pool.setMaxConn( 250 );
		pool.setMaintSleep( 30 );
		pool.setNagle( false );
		pool.setSocketTO( 3000 );
		pool.setAliveCheck( true );
		pool.initialize();

		MemCachedClient memCachedClient = new MemCachedClient();

		// turn off most memcached client logging:
		com.danga.MemCached.Logger.getLogger( MemCachedClient.class.getName() ).setLevel( com.danga.MemCached.Logger.LEVEL_WARN );

		for ( int i = 0; i < 10; i++ ) {
			boolean success = memCachedClient.set( "" + i, "Hello!" );
			String result = (String)memCachedClient.get( "" + i );
			System.out.println( String.format( "set( %d ): %s", i, success ) );
			System.out.println( String.format( "get( %d ): %s", i, result ) );
		}

		System.out.println( "\n\t -- sleeping --\n" );
		try { Thread.sleep( 10000 ); } catch ( Exception ex ) { }

		for ( int i = 0; i < 10; i++ ) {
			boolean success = memCachedClient.set( "" + i, "Hello!" );
			String result = (String)memCachedClient.get( "" + i );
			System.out.println( String.format( "set( %d ): %s", i, success ) );
			System.out.println( String.format( "get( %d ): %s", i, result ) );
		}
	}
}
