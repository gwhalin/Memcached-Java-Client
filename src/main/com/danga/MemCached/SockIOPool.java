/**
 * Copyright (c) 2008 Greg Whalin
 * All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the BSD license
 *
 * This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.
 *
 * You should have received a copy of the BSD License along with this
 * library.
 *
 * @author greg whalin <greg@meetup.com> 
 */

/*******************************************************************************
 * Copyright (c) 2009 Schooner Information Technology, Inc.
 * All rights reserved.
 * 
 * http://www.schoonerinfotech.com/
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.danga.MemCached;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import com.schooner.MemCached.SchoonerSockIOPool;

/**
 * This class is a connection pool for maintaning a pool of persistent
 * connections<br/>
 * to memcached servers.
 * 
 * The pool must be initialized prior to use. This should typically be early on<br/>
 * in the lifecycle of the JVM instance.<br/>
 * <br/>
 * <h3>An example of initializing using defaults:</h3>
 * 
 * <pre>
 * 
 * static {
 * 	String[] serverlist = { &quot;cache0.server.com:12345&quot;, &quot;cache1.server.com:12345&quot; };
 * 	SockIOPool pool = SockIOPool.getInstance();
 * 	pool.setServers(serverlist);
 * 	pool.initialize();
 * }
 * </pre>
 * 
 * <h3>An example of initializing using defaults and providing weights for
 * servers:</h3>
 * 
 * <pre>
 * static {
 * 	String[] serverlist = { &quot;cache0.server.com:12345&quot;, &quot;cache1.server.com:12345&quot; };
 * 	Integer[] weights = { new Integer(5), new Integer(2) };
 * 
 * 	SockIOPool pool = SockIOPool.getInstance();
 * 	pool.setServers(serverlist);
 * 	pool.setWeights(weights);
 * 	pool.initialize();
 * }
 * </pre>
 * 
 * <h3>An example of initializing overriding defaults:</h3>
 * 
 * <pre>
 * static {
 * 	String[] serverlist = { &quot;cache0.server.com:12345&quot;, &quot;cache1.server.com:12345&quot; };
 * 	Integer[] weights = { new Integer(5), new Integer(2) };
 * 	int initialConnections = 10;
 * 	int minSpareConnections = 5;
 * 	int maxSpareConnections = 50;
 * 	long maxIdleTime = 1000 * 60 * 30; // 30 minutes
 * 	long maxBusyTime = 1000 * 60 * 5; // 5 minutes
 * 	long maintThreadSleep = 1000 * 5; // 5 seconds
 * 	int socketTimeOut = 1000 * 3; // 3 seconds to block on reads
 * 	int socketConnectTO = 1000 * 3; // 3 seconds to block on initial connections.  If 0, then will use blocking connect (default)
 * 	boolean failover = false; // turn off auto-failover in event of server down	
 * 	boolean nagleAlg = false; // turn off Nagle's algorithm on all sockets in pool	
 * 	boolean aliveCheck = false; // disable health check of socket on checkout
 * 	SockIOPool pool = SockIOPool.getInstance();
 * 	pool.setServers(serverlist);
 * 	pool.setWeights(weights);
 * 	pool.setInitConn(initialConnections);
 * 	pool.setMinConn(minSpareConnections);
 * 	pool.setMaxConn(maxSpareConnections);
 * 	pool.setMaxIdle(maxIdleTime);
 * 	pool.setMaxBusyTime(maxBusyTime);
 * 	pool.setMaintSleep(maintThreadSleep);
 * 	pool.setSocketTO(socketTimeOut);
 * 	pool.setNagle(nagleAlg);
 * 	pool.setHashingAlg(SockIOPool.NEW_COMPAT_HASH);
 * 	pool.setAliveCheck(true);
 * 	pool.initialize();
 * }
 * </pre>
 * 
 * The easiest manner in which to initialize the pool is to set the servers and
 * rely on defaults as in the first example.<br/>
 * After pool is initialized, a client will request a SockIO object by calling
 * getSock with the cache key<br/>
 * The client must always close the SockIO object when finished, which will
 * return the connection back to the pool.<br/>
 * <h3>An example of retrieving a SockIO object:</h3>
 * 
 * <pre>
 * 	SockIOPool.SockIO sock = SockIOPool.getInstance().getSock( key );
 * 	try {
 * 		sock.write( "version\r\n" );	
 * 		sock.flush();	
 * 		System.out.println( "Version: " + sock.readLine() );	
 * 	}
 * 	catch (IOException ioe) { System.out.println( "io exception thrown" ) };	
 * 	sock.close();
 * </pre>
 * 
 * @author greg whalin <greg@whalin.com>
 * @version 1.5
 */
public class SockIOPool {

	// logger
	private static Logger log = Logger.getLogger(SockIOPool.class.getName());

	// Constants
	private static final Integer ZERO = new Integer(0);
	// Pool data
	// is initialized

	// initial, min and max pool sizes
	private int poolMultiplier = 3;
	private int minConn = 5;
	private int maxConn = 100;
	private long maxIdle = 1000 * 60 * 5; // max idle time for avail sockets
	private long maxBusyTime = 1000 * 30; // max idle time for avail sockets
	private int socketTO = 1000 * 3; // default timeout of socket reads
	private int socketConnectTO = 1000 * 3; // default timeout of socket
	// connections
	// for being alive
	private boolean failover = true; // default to failover in event of cache
	// server dead
	private boolean failback = true; // only used if failover is also set ...
	// controls putting a dead server back
	// into rotation
	private boolean nagle = false; // enable/disable Nagle's algorithm
	// it is the fastest

	// locks
	private final ReentrantLock hostDeadLock = new ReentrantLock();

	// dead server map
	private Map<String, Date> hostDead;
	private Map<String, Long> hostDeadDur;

	// map to hold all available sockets
	// map to hold busy sockets
	// set to hold sockets to close
	private Map<String, Map<SockIO, Long>> availPool;
	private Map<String, Map<SockIO, Long>> busyPool;
	private Map<SockIO, Integer> deadPool;;

	private SchoonerSockIOPool schoonerSockIOPool;

	// Constants

	// native String.hashCode();
	public static final int NATIVE_HASH = SchoonerSockIOPool.NATIVE_HASH;
	// original compatibility hashing algorithm (works with other clients)
	public static final int OLD_COMPAT_HASH = SchoonerSockIOPool.OLD_COMPAT_HASH;
	// new CRC32 based compatibility hashing algorithm (works with other
	// clients)
	public static final int NEW_COMPAT_HASH = SchoonerSockIOPool.NEW_COMPAT_HASH;
	// MD5 Based -- Stops thrashing when a server added or removed
	public static final int CONSISTENT_HASH = SchoonerSockIOPool.CONSISTENT_HASH;
	// max of 10 minute delay for fall off
	public static final long MAX_RETRY_DELAY = SchoonerSockIOPool.MAX_RETRY_DELAY;

	// empty constructor
	protected SockIOPool() {
	}

	/**
	 * Factory to create/retrieve new pools given a unique poolName.
	 * 
	 * @param poolName
	 *            unique name of the pool
	 * @return instance of SockIOPool
	 */
	public static synchronized SockIOPool getInstance(String poolName) {
		SockIOPool whalinSockIOPool = new SockIOPool();
		whalinSockIOPool.schoonerSockIOPool = SchoonerSockIOPool.getInstance(poolName);
		return whalinSockIOPool;
	}

	public static SockIOPool getInstance(boolean isTcp) {
		SockIOPool whalinSockIOPool = new SockIOPool();
		whalinSockIOPool.schoonerSockIOPool = SchoonerSockIOPool.getInstance(isTcp);
		return whalinSockIOPool;
	}

	public static SockIOPool getInstance(String poolName, boolean isTcp) {
		SockIOPool whalinSockIOPool = new SockIOPool();
		whalinSockIOPool.schoonerSockIOPool = SchoonerSockIOPool.getInstance(poolName, isTcp);
		return whalinSockIOPool;
	}

	/**
	 * Single argument version of factory used for back compat. Simply creates a
	 * pool named "default".
	 * 
	 * @return instance of SockIOPool
	 */
	public static SockIOPool getInstance() {
		SockIOPool whalinSockIOPool = new SockIOPool();
		whalinSockIOPool.schoonerSockIOPool = SchoonerSockIOPool.getInstance("default");
		return whalinSockIOPool;
	}

	/**
	 * Sets the list of all cache servers.
	 * 
	 * @param servers
	 *            String array of servers [host:port]
	 */
	public void setServers(String[] servers) {
		schoonerSockIOPool.setServers(servers);
	}

	/**
	 * Returns the current list of all cache servers.
	 * 
	 * @return String array of servers [host:port]
	 */
	public String[] getServers() {
		return schoonerSockIOPool.getServers();
	}

	/**
	 * Sets the list of weights to apply to the server list.
	 * 
	 * This is an int array with each element corresponding to an element<br/>
	 * in the same position in the server String array.
	 * 
	 * @param weights
	 *            Integer array of weights
	 */
	public void setWeights(Integer[] weights) {
		schoonerSockIOPool.setWeights(weights);
	}

	/**
	 * Returns the current list of weights.
	 * 
	 * @return int array of weights
	 */
	public Integer[] getWeights() {
		return schoonerSockIOPool.getWeights();
	}

	/**
	 * Sets the initial number of connections per server in the available pool.
	 * 
	 * @param initConn
	 *            int number of connections
	 */
	public void setInitConn(int initConn) {
		schoonerSockIOPool.setInitConn(initConn);
	}

	/**
	 * Returns the current setting for the initial number of connections per
	 * server in the available pool.
	 * 
	 * @return number of connections
	 */
	public int getInitConn() {
		return schoonerSockIOPool.getInitConn();
	}

	/**
	 * Sets the minimum number of spare connections to maintain in our available
	 * pool.
	 * 
	 * @param minConn
	 *            number of connections
	 */
	public void setMinConn(int minConn) {
		schoonerSockIOPool.setMinConn(minConn);
	}

	/**
	 * Returns the minimum number of spare connections in available pool.
	 * 
	 * @return number of connections
	 */
	public int getMinConn() {
		return schoonerSockIOPool.getMinConn();
	}

	/**
	 * Sets the maximum number of spare connections allowed in our available
	 * pool.
	 * 
	 * @param maxConn
	 *            number of connections
	 */
	public void setMaxConn(int maxConn) {
		schoonerSockIOPool.setMaxConn(maxConn);
	}

	/**
	 * Returns the maximum number of spare connections allowed in available
	 * pool.
	 * 
	 * @return number of connections
	 */
	public int getMaxConn() {
		return schoonerSockIOPool.getMaxConn();
	}

	/**
	 * Sets the max busy time for threads in the busy pool.
	 * 
	 * @param maxBusyTime
	 *            idle time in ms
	 */
	public void setMaxBusyTime(long maxBusyTime) {
		schoonerSockIOPool.setMaxBusyTime(maxBusyTime);
	}

	/**
	 * Returns the current max busy setting.
	 * 
	 * @return max busy setting in ms
	 */
	public long getMaxBusy() {
		return schoonerSockIOPool.getMaxBusy();
	}

	/**
	 * Sets the socket timeout for reads.
	 * 
	 * @param socketTO
	 *            timeout in ms
	 */
	public void setSocketTO(int socketTO) {
		schoonerSockIOPool.setSocketTO(socketTO);
	}

	/**
	 * Returns the socket timeout for reads.
	 * 
	 * @return timeout in ms
	 */
	public int getSocketTO() {
		return schoonerSockIOPool.getSocketTO();
	}

	/**
	 * Sets the socket timeout for connect.
	 * 
	 * @param socketConnectTO
	 *            timeout in ms
	 */
	public void setSocketConnectTO(int socketConnectTO) {
		schoonerSockIOPool.setSocketConnectTO(socketConnectTO);
	}

	/**
	 * Returns the socket timeout for connect.
	 * 
	 * @return timeout in ms
	 */
	public int getSocketConnectTO() {
		return schoonerSockIOPool.getSocketTO();
	}

	/**
	 * Sets the max idle time for threads in the available pool.
	 * 
	 * @param maxIdle
	 *            idle time in ms
	 */
	public void setMaxIdle(long maxIdle) {
		schoonerSockIOPool.setMaxIdle(maxIdle);
	}

	/**
	 * Returns the current max idle setting.
	 * 
	 * @return max idle setting in ms
	 */
	public long getMaxIdle() {
		return schoonerSockIOPool.getMaxIdle();
	}

	/**
	 * Set the sleep time between runs of the pool maintenance thread. If set to
	 * 0, then the maint thread will not be started.
	 * 
	 * @param maintSleep
	 *            sleep time in ms
	 */
	public void setMaintSleep(long maintSleep) {
		schoonerSockIOPool.setMaintSleep(maintSleep);
	}

	/**
	 * Returns the current maint thread sleep time.
	 * 
	 * @return sleep time in ms
	 */
	public long getMaintSleep() {
		return schoonerSockIOPool.getMaintSleep();
	}

	/**
	 * Sets the failover flag for the pool.
	 * 
	 * If this flag is set to true, and a socket fails to connect,<br/>
	 * the pool will attempt to return a socket from another server<br/>
	 * if one exists. If set to false, then getting a socket<br/>
	 * will return null if it fails to connect to the requested server.
	 * 
	 * @param failover
	 *            true/false
	 */
	public void setFailover(boolean failover) {
		schoonerSockIOPool.setFailover(failover);
	}

	/**
	 * Returns current state of failover flag.
	 * 
	 * @return true/false
	 */
	public boolean getFailover() {
		return schoonerSockIOPool.getFailover();
	}

	/**
	 * Sets the failback flag for the pool.
	 * 
	 * If this is true and we have marked a host as dead, will try to bring it
	 * back. If it is false, we will never try to resurrect a dead host.
	 * 
	 * @param failback
	 *            true/false
	 */
	public void setFailback(boolean failback) {
		schoonerSockIOPool.setFailback(failback);
	}

	/**
	 * Returns current state of failover flag.
	 * 
	 * @return true/false
	 */
	public boolean getFailback() {
		return schoonerSockIOPool.getFailback();
	}

	/**
	 * Sets the aliveCheck flag for the pool.
	 * 
	 * When true, this will attempt to talk to the server on every connection
	 * checkout to make sure the connection is still valid. This adds extra
	 * network chatter and thus is defaulted off. May be useful if you want to
	 * ensure you do not have any problems talking to the server on a dead
	 * connection.
	 * 
	 * @param aliveCheck
	 *            true/false
	 */
	public void setAliveCheck(boolean aliveCheck) {
		schoonerSockIOPool.setAliveCheck(aliveCheck);
	}

	/**
	 * Returns the current status of the aliveCheck flag.
	 * 
	 * @return true / false
	 */
	public boolean getAliveCheck() {
		return schoonerSockIOPool.getAliveCheck();
	}

	/**
	 * Sets the Nagle alg flag for the pool.
	 * 
	 * If false, will turn off Nagle's algorithm on all sockets created.
	 * 
	 * @param nagle
	 *            true/false
	 */
	public void setNagle(boolean nagle) {
		schoonerSockIOPool.setNagle(nagle);
	}

	/**
	 * Returns current status of nagle flag
	 * 
	 * @return true/false
	 */
	public boolean getNagle() {
		return schoonerSockIOPool.getNagle();
	}

	/**
	 * Sets the hashing algorithm we will use.
	 * 
	 * The types are as follows.
	 * 
	 * SockIOPool.NATIVE_HASH (0) - native String.hashCode() - fast (cached) but
	 * not compatible with other clients SockIOPool.OLD_COMPAT_HASH (1) -
	 * original compatibility hashing alg (works with other clients)
	 * SockIOPool.NEW_COMPAT_HASH (2) - new CRC32 based compatibility hashing
	 * algorithm (fast and works with other clients)
	 * 
	 * @param alg
	 *            int value representing hashing algorithm
	 */
	public void setHashingAlg(int alg) {
		schoonerSockIOPool.setHashingAlg(alg);
	}

	/**
	 * Returns current status of customHash flag
	 * 
	 * @return true/false
	 */
	public int getHashingAlg() {
		return schoonerSockIOPool.getHashingAlg();
	}

	/**
	 * Initializes the pool.
	 */
	public void initialize() {
		schoonerSockIOPool.initialize();
	}

	/**
	 * Returns state of pool.
	 * 
	 * @return <CODE>true</CODE> if initialized.
	 */
	public boolean isInitialized() {
		return schoonerSockIOPool.isInitialized();
	}

	/**
	 * @param key
	 * @return
	 */
	public String getHost(String key) {
		return schoonerSockIOPool.getHost(key);
	}

	/**
	 * Gets the host that a particular key / hashcode resides on.
	 * 
	 * @param key
	 * @param hashcode
	 * @return
	 */
	public String getHost(String key, Integer hashcode) {
		return schoonerSockIOPool.getHost(key, hashcode);
	}

	/**
	 * Shuts down the pool.
	 * 
	 * Cleanly closes all sockets.<br/>
	 * Stops the maint thread.<br/>
	 * Nulls out all internal maps<br/>
	 */
	public void shutDown() {
		schoonerSockIOPool.shutDown();
	}

	/**
	 * In memcached 1.4+, user can specify memory size for each memcached item.<br>
	 * You can specify it with parameter "-I" in the server side.<br>
	 * While in our client side, we make the max memcached item size with the
	 * default value 1Mb.<br>
	 * In this scenario, you can extend the size as what you want, please be
	 * sure you have enough memory in the client side, since we are using direct
	 * buffer to hold the memory, they will not be free until shutdown.
	 * 
	 * @param bufferSize
	 *            specified buffer size.
	 */
	public void setBufferSize(int bufferSize) {
		schoonerSockIOPool.setBufferSize(bufferSize);
	}

	public int getBufferSize() {
		return schoonerSockIOPool.getBufferSize();
	}

	/**
	 * MemCached client for Java, utility class for Socket IO.
	 * 
	 * This class is a wrapper around a Socket and its streams.
	 * 
	 * @author greg whalin <greg@meetup.com>
	 * @author Richard 'toast' Russo <russor@msoe.edu>
	 * @version 1.5
	 */
	public static class SockIO implements LineInputStream {

		// logger
		private static Logger log = Logger.getLogger(SockIO.class.getName());

		// pool
		private SockIOPool pool;

		// data
		private String host;
		private Socket sock;

		private DataInputStream in;
		private BufferedOutputStream out;

		/**
		 * creates a new SockIO object wrapping a socket connection to
		 * host:port, and its input and output streams
		 * 
		 * @param pool
		 *            Pool this object is tied to
		 * @param host
		 *            host to connect to
		 * @param port
		 *            port to connect to
		 * @param timeout
		 *            int ms to block on data for read
		 * @param connectTimeout
		 *            timeout (in ms) for initial connection
		 * @param noDelay
		 *            TCP NODELAY option?
		 * @throws IOException
		 *             if an io error occurrs when creating socket
		 * @throws UnknownHostException
		 *             if hostname is invalid
		 */
		public SockIO(SockIOPool pool, String host, int port, int timeout, int connectTimeout, boolean noDelay)
				throws IOException, UnknownHostException {

			this.pool = pool;

			// get a socket channel
			sock = getSocket(host, port, connectTimeout);

			if (timeout >= 0)
				sock.setSoTimeout(timeout);

			// testing only
			sock.setTcpNoDelay(noDelay);

			// wrap streams
			in = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
			out = new BufferedOutputStream(sock.getOutputStream());

			this.host = host + ":" + port;
		}

		/**
		 * creates a new SockIO object wrapping a socket connection to
		 * host:port, and its input and output streams
		 * 
		 * @param host
		 *            hostname:port
		 * @param timeout
		 *            read timeout value for connected socket
		 * @param connectTimeout
		 *            timeout for initial connections
		 * @param noDelay
		 *            TCP NODELAY option?
		 * @throws IOException
		 *             if an io error occurrs when creating socket
		 * @throws UnknownHostException
		 *             if hostname is invalid
		 */
		public SockIO(SockIOPool pool, String host, int timeout, int connectTimeout, boolean noDelay)
				throws IOException, UnknownHostException {
			if (pool == null)
				return;
			this.pool = pool;

			String[] ip = host.split(":");

			// get socket: default is to use non-blocking connect
			sock = getSocket(ip[0], Integer.parseInt(ip[1]), connectTimeout);

			if (timeout >= 0)
				this.sock.setSoTimeout(timeout);

			// testing only
			sock.setTcpNoDelay(noDelay);

			// wrap streams
			in = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
			out = new BufferedOutputStream(sock.getOutputStream());

			this.host = host;
		}

		/**
		 * Method which gets a connection from SocketChannel.
		 * 
		 * @param host
		 *            host to establish connection to
		 * @param port
		 *            port on that host
		 * @param timeout
		 *            connection timeout in ms
		 * 
		 * @return connected socket
		 * @throws IOException
		 *             if errors connecting or if connection times out
		 */
		protected static Socket getSocket(String host, int port, int timeout) throws IOException {
			SocketChannel sock = SocketChannel.open();
			sock.socket().connect(new InetSocketAddress(host, port), timeout);
			return sock.socket();
		}

		/**
		 * Lets caller get access to underlying channel.
		 * 
		 * @return the backing SocketChannel
		 */
		public SocketChannel getChannel() {
			return sock.getChannel();
		}

		/**
		 * returns the host this socket is connected to
		 * 
		 * @return String representation of host (hostname:port)
		 */
		public String getHost() {
			return this.host;
		}

		/**
		 * closes socket and all streams connected to it
		 * 
		 * @throws IOException
		 *             if fails to close streams or socket
		 */
		public void trueClose() throws IOException {
			trueClose(true);
		}

		/**
		 * closes socket and all streams connected to it
		 * 
		 * @throws IOException
		 *             if fails to close streams or socket
		 */
		public void trueClose(boolean addToDeadPool) throws IOException {
			if (log.isDebugEnabled())
				log.debug("++++ Closing socket for real: " + toString());

			boolean err = false;
			StringBuilder errMsg = new StringBuilder();

			if (in != null) {
				try {
					in.close();
				} catch (IOException ioe) {
					log.error("++++ error closing input stream for socket: " + toString() + " for host: " + getHost());
					log.error(ioe.getMessage(), ioe);
					errMsg.append("++++ error closing input stream for socket: " + toString() + " for host: "
							+ getHost() + "\n");
					errMsg.append(ioe.getMessage());
					err = true;
				}
			}

			if (out != null) {
				try {
					out.close();
				} catch (IOException ioe) {
					log.error("++++ error closing output stream for socket: " + toString() + " for host: " + getHost());
					log.error(ioe.getMessage(), ioe);
					errMsg.append("++++ error closing output stream for socket: " + toString() + " for host: "
							+ getHost() + "\n");
					errMsg.append(ioe.getMessage());
					err = true;
				}
			}

			if (sock != null) {
				try {
					sock.close();
				} catch (IOException ioe) {
					log.error("++++ error closing socket: " + toString() + " for host: " + getHost());
					log.error(ioe.getMessage(), ioe);
					errMsg.append("++++ error closing socket: " + toString() + " for host: " + getHost() + "\n");
					errMsg.append(ioe.getMessage());
					err = true;
				}
			}

			// check in to pool
			if (addToDeadPool && sock != null)
				pool.checkIn(this, false);

			in = null;
			out = null;
			sock = null;

			if (err)
				throw new IOException(errMsg.toString());
		}

		/**
		 * sets closed flag and checks in to connection pool but does not close
		 * connections
		 */
		public void close() {
			// check in to pool
			if (log.isDebugEnabled())
				log.debug("++++ marking socket (" + this.toString()
						+ ") as closed and available to return to avail pool");
			pool.checkIn(this);
		}

		/**
		 * checks if the connection is open
		 * 
		 * @return true if connected
		 */
		protected boolean isConnected() {
			return (sock != null && sock.isConnected());
		}

		/*
		 * checks to see that the connection is still working
		 * 
		 * @return true if still alive
		 */
		public boolean isAlive() {

			if (!isConnected())
				return false;

			// try to talk to the server w/ a dumb query to ask its version
			try {
				this.write("version\r\n".getBytes());
				this.flush();
				this.readLine();
			} catch (IOException ex) {
				return false;
			}

			return true;
		}

		/**
		 * reads a line intentionally not using the deprecated readLine method
		 * from DataInputStream
		 * 
		 * @return String that was read in
		 * @throws IOException
		 *             if io problems during read
		 */
		public String readLine() throws IOException {
			if (sock == null || !sock.isConnected()) {
				log.error("++++ attempting to read from closed socket");
				throw new IOException("++++ attempting to read from closed socket");
			}

			byte[] b = new byte[1];
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			boolean eol = false;

			while (in.read(b, 0, 1) != -1) {

				if (b[0] == 13) {
					eol = true;
				} else {
					if (eol) {
						if (b[0] == 10)
							break;

						eol = false;
					}
				}

				// cast byte into char array
				bos.write(b, 0, 1);
			}

			if (bos == null || bos.size() <= 0) {
				throw new IOException("++++ Stream appears to be dead, so closing it down");
			}

			// else return the string
			return bos.toString().trim();
		}

		/**
		 * reads up to end of line and returns nothing
		 * 
		 * @throws IOException
		 *             if io problems during read
		 */
		public void clearEOL() throws IOException {
			if (sock == null || !sock.isConnected()) {
				log.error("++++ attempting to read from closed socket");
				throw new IOException("++++ attempting to read from closed socket");
			}

			byte[] b = new byte[1];
			boolean eol = false;
			while (in.read(b, 0, 1) != -1) {

				// only stop when we see
				// \r (13) followed by \n (10)
				if (b[0] == 13) {
					eol = true;
					continue;
				}

				if (eol) {
					if (b[0] == 10)
						break;

					eol = false;
				}
			}
		}

		/**
		 * reads length bytes into the passed in byte array from dtream
		 * 
		 * @param b
		 *            byte array
		 * @throws IOException
		 *             if io problems during read
		 */
		public int read(byte[] b) throws IOException {
			if (sock == null || !sock.isConnected()) {
				log.error("++++ attempting to read from closed socket");
				throw new IOException("++++ attempting to read from closed socket");
			}

			int count = 0;
			while (count < b.length) {
				int cnt = in.read(b, count, (b.length - count));
				count += cnt;
			}

			return count;
		}

		/**
		 * flushes output stream
		 * 
		 * @throws IOException
		 *             if io problems during read
		 */
		public void flush() throws IOException {
			if (sock == null || !sock.isConnected()) {
				log.error("++++ attempting to write to closed socket");
				throw new IOException("++++ attempting to write to closed socket");
			}
			out.flush();
		}

		/**
		 * writes a byte array to the output stream
		 * 
		 * @param b
		 *            byte array to write
		 * @throws IOException
		 *             if an io error happens
		 */
		public void write(byte[] b) throws IOException {
			if (sock == null || !sock.isConnected()) {
				log.error("++++ attempting to write to closed socket");
				throw new IOException("++++ attempting to write to closed socket");
			}
			out.write(b);
		}

		/**
		 * use the sockets hashcode for this object so we can key off of SockIOs
		 * 
		 * @return int hashcode
		 */
		public int hashCode() {
			return (sock == null) ? 0 : sock.hashCode();
		}

		/**
		 * returns the string representation of this socket
		 * 
		 * @return string
		 */
		public String toString() {
			return (sock == null) ? "" : sock.toString();
		}

		/**
		 * Hack to reap any leaking children.
		 */
		protected void finalize() throws Throwable {
			try {
				if (sock != null) {
					log.error("++++ closing potentially leaked socket in finalize");
					sock.close();
					sock = null;
				}
			} catch (Throwable t) {
				log.error(t.getMessage(), t);
			} finally {
				super.finalize();
			}
		}
	}

	/**
	 * Returns appropriate SockIO object given string cache key.
	 * 
	 * @param key
	 *            hashcode for cache key
	 * @return SockIO obj connected to server
	 */
	public SockIO getSock(String key) {
		return schoonerSockIOPool.getSock(key);
	}

	/**
	 * Returns appropriate SockIO object given string cache key and optional
	 * hashcode.
	 * 
	 * Trys to get SockIO from pool. Fails over to additional pools in event of
	 * server failure.
	 * 
	 * @param key
	 *            hashcode for cache key
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return SockIO obj connected to server
	 */
	public SockIO getSock(String key, Integer hashCode) {
		return schoonerSockIOPool.getSock(key, hashCode);
	}

	/**
	 * Returns a SockIO object from the pool for the passed in host.
	 * 
	 * Meant to be called from a more intelligent method<br/>
	 * which handles choosing appropriate server<br/>
	 * and failover.
	 * 
	 * @param host
	 *            host from which to retrieve object
	 * @return SockIO object or null if fail to retrieve one
	 */
	public SockIO getConnection(String host) {
		return schoonerSockIOPool.getConnection(host);
	}

	/**
	 * Returns a socket to the avail pool.
	 * 
	 * This is called from SockIO.close(). Calling this method<br/>
	 * directly without closing the SockIO object first<br/>
	 * will cause an IOException to be thrown.
	 * 
	 * @param socket
	 *            socket to return
	 */
	private void checkIn(SockIO socket) {
		checkIn(socket, true);
	}

	/**
	 * Checks a SockIO object in with the pool.
	 * 
	 * This will remove SocketIO from busy pool, and optionally<br/>
	 * add to avail pool.
	 * 
	 * @param socket
	 *            socket to return
	 * @param addToAvail
	 *            add to avail pool if true
	 */
	private void checkIn(SockIO socket, boolean addToAvail) {

		String host = socket.getHost();
		if (log.isDebugEnabled())
			log.debug("++++ calling check-in on socket: " + socket.toString() + " for host: " + host);

		synchronized (this) {
			// remove from the busy pool
			if (log.isDebugEnabled())
				log.debug("++++ removing socket (" + socket.toString() + ") from busy pool for host: " + host);
			removeSocketFromPool(busyPool, host, socket);

			if (socket.isConnected() && addToAvail) {
				// add to avail pool
				if (log.isDebugEnabled())
					log.debug("++++ returning socket (" + socket.toString() + " to avail pool for host: " + host);
				addSocketToPool(availPool, host, socket);
			} else {
				deadPool.put(socket, ZERO);
				socket = null;
			}
		}
	}

	/**
	 * Class which extends thread and handles maintenance of the pool.
	 * 
	 * @author greg whalin <greg@meetup.com>
	 * @version 1.5
	 */
	protected static class MaintThread extends Thread {

		private SockIOPool pool;
		private long interval = 1000 * 3; // every 3 seconds
		private boolean stopThread = false;
		private boolean running;

		protected MaintThread(SockIOPool pool) {
			this.pool = pool;
			this.setDaemon(true);
			this.setName("MaintThread");
		}

		public void setInterval(long interval) {
			this.interval = interval;
		}

		public boolean isRunning() {
			return this.running;
		}

		/**
		 * sets stop variable and interupts any wait
		 */
		public void stopThread() {
			this.stopThread = true;
			this.interrupt();
		}

		/**
		 * Start the thread.
		 */
		public void run() {
			this.running = true;

			while (!this.stopThread) {
				try {
					Thread.sleep(interval);

					// if pool is initialized, then
					// run the maintenance method on itself
					if (pool.isInitialized())
						pool.selfMaint();

				} catch (Exception e) {
					break;
				}
			}

			this.running = false;
		}
	}

	/**
	 * Creates a new SockIO obj for the given server.
	 * 
	 * If server fails to connect, then return null and do not try<br/>
	 * again until a duration has passed. This duration will grow<br/>
	 * by doubling after each failed attempt to connect.
	 * 
	 * @param host
	 *            host:port to connect to
	 * @return SockIO obj or null if failed to create
	 */
	protected SockIO createSocket(String host) {

		SockIO socket = null;

		// if host is dead, then we don't need to try again
		// until the dead status has expired
		// we do not try to put back in if failback is off
		hostDeadLock.lock();
		try {
			if (failover && failback && hostDead.containsKey(host) && hostDeadDur.containsKey(host)) {

				Date store = hostDead.get(host);
				long expire = hostDeadDur.get(host).longValue();

				if ((store.getTime() + expire) > System.currentTimeMillis())
					return null;
			}
		} finally {
			hostDeadLock.unlock();
		}

		try {
			socket = new SockIO(this, host, this.socketTO, this.socketConnectTO, this.nagle);

			if (!socket.isConnected()) {
				log.error("++++ failed to get SockIO obj for: " + host + " -- new socket is not connected");
				deadPool.put(socket, ZERO);
				socket = null;
			}
		} catch (Exception ex) {
			log.error("++++ failed to get SockIO obj for: " + host);
			log.error(ex.getMessage(), ex);
			socket = null;
		}

		// if we failed to get socket, then mark
		// host dead for a duration which falls off
		hostDeadLock.lock();
		try {
			if (socket == null) {
				Date now = new Date();
				hostDead.put(host, now);

				long expire = (hostDeadDur.containsKey(host)) ? (((Long) hostDeadDur.get(host)).longValue() * 2) : 1000;

				if (expire > MAX_RETRY_DELAY)
					expire = MAX_RETRY_DELAY;

				hostDeadDur.put(host, new Long(expire));
				if (log.isDebugEnabled())
					log.debug("++++ ignoring dead host: " + host + " for " + expire + " ms");

				// also clear all entries for this host from availPool
				clearHostFromPool(availPool, host);
			} else {
				if (log.isDebugEnabled())
					log.debug("++++ created socket (" + socket.toString() + ") for host: " + host);
				if (hostDead.containsKey(host) || hostDeadDur.containsKey(host)) {
					hostDead.remove(host);
					hostDeadDur.remove(host);
				}
			}
		} finally {
			hostDeadLock.unlock();
		}

		return socket;
	}

	/**
	 * Runs self maintenance on all internal pools.
	 * 
	 * This is typically called by the maintenance thread to manage pool size.
	 */
	protected void selfMaint() {
		if (log.isDebugEnabled())
			log.debug("++++ Starting self maintenance....");

		// go through avail sockets and create sockets
		// as needed to maintain pool settings
		Map<String, Integer> needSockets = new HashMap<String, Integer>();

		synchronized (this) {
			// find out how many to create
			for (Iterator<String> i = availPool.keySet().iterator(); i.hasNext();) {
				String host = i.next();
				Map<SockIO, Long> sockets = availPool.get(host);

				if (log.isDebugEnabled())
					log.debug("++++ Size of avail pool for host (" + host + ") = " + sockets.size());

				// if pool is too small (n < minSpare)
				if (sockets.size() < minConn) {
					// need to create new sockets
					int need = minConn - sockets.size();
					needSockets.put(host, need);
				}
			}
		}

		// now create
		Map<String, Set<SockIO>> newSockets = new HashMap<String, Set<SockIO>>();

		for (String host : needSockets.keySet()) {
			Integer need = needSockets.get(host);

			if (log.isDebugEnabled())
				log.debug("++++ Need to create " + need + " new sockets for pool for host: " + host);

			Set<SockIO> newSock = new HashSet<SockIO>(need);
			for (int j = 0; j < need; j++) {
				SockIO socket = createSocket(host);

				if (socket == null)
					break;

				newSock.add(socket);
			}

			newSockets.put(host, newSock);
		}

		// synchronize to add and remove to/from avail pool
		// as well as clean up the busy pool (no point in releasing
		// lock here as should be quick to pool adjust and no
		// blocking ops here)
		synchronized (this) {
			for (String host : newSockets.keySet()) {
				Set<SockIO> sockets = newSockets.get(host);
				for (SockIO socket : sockets)
					addSocketToPool(availPool, host, socket);
			}

			for (Iterator<String> i = availPool.keySet().iterator(); i.hasNext();) {
				String host = i.next();
				Map<SockIO, Long> sockets = availPool.get(host);
				if (log.isDebugEnabled())
					log.debug("++++ Size of avail pool for host (" + host + ") = " + sockets.size());

				if (sockets.size() > maxConn) {
					// need to close down some sockets
					int diff = sockets.size() - maxConn;
					int needToClose = (diff <= poolMultiplier) ? diff : (diff) / poolMultiplier;

					if (log.isDebugEnabled())
						log.debug("++++ need to remove " + needToClose + " spare sockets for pool for host: " + host);

					for (Iterator<SockIO> j = sockets.keySet().iterator(); j.hasNext();) {
						if (needToClose <= 0)
							break;

						// remove stale entries
						SockIO socket = j.next();
						long expire = sockets.get(socket).longValue();

						// if past idle time
						// then close socket
						// and remove from pool
						if ((expire + maxIdle) < System.currentTimeMillis()) {
							if (log.isDebugEnabled())
								log
										.debug("+++ removing stale entry from pool as it is past its idle timeout and pool is over max spare");

							// remove from the availPool
							deadPool.put(socket, ZERO);
							j.remove();
							needToClose--;
						}
					}
				}
			}

			// go through busy sockets and destroy sockets
			// as needed to maintain pool settings
			for (Iterator<String> i = busyPool.keySet().iterator(); i.hasNext();) {

				String host = i.next();
				Map<SockIO, Long> sockets = busyPool.get(host);

				if (log.isDebugEnabled())
					log.debug("++++ Size of busy pool for host (" + host + ")  = " + sockets.size());

				// loop through all connections and check to see if we have any
				// hung connections
				for (Iterator<SockIO> j = sockets.keySet().iterator(); j.hasNext();) {
					// remove stale entries
					SockIO socket = j.next();
					long hungTime = sockets.get(socket).longValue();

					// if past max busy time
					// then close socket
					// and remove from pool
					if ((hungTime + maxBusyTime) < System.currentTimeMillis()) {
						log.error("+++ removing potentially hung connection from busy pool ... socket in pool for "
								+ (System.currentTimeMillis() - hungTime) + "ms");

						// remove from the busy pool
						deadPool.put(socket, ZERO);
						j.remove();
					}
				}
			}
		}

		// finally clean out the deadPool
		Set<SockIO> toClose;
		synchronized (deadPool) {
			toClose = deadPool.keySet();
			deadPool = new IdentityHashMap<SockIO, Integer>();
		}

		for (SockIO socket : toClose) {
			try {
				socket.trueClose(false);
			} catch (Exception ex) {
				log.error("++++ failed to close SockIO obj from deadPool");
				log.error(ex.getMessage(), ex);
			}

			socket = null;
		}

		if (log.isDebugEnabled())
			log.debug("+++ ending self maintenance.");
	}

	/**
	 * Adds a socket to a given pool for the given host. THIS METHOD IS NOT
	 * THREADSAFE, SO BE CAREFUL WHEN USING!
	 * 
	 * Internal utility method.
	 * 
	 * @param pool
	 *            pool to add to
	 * @param host
	 *            host this socket is connected to
	 * @param socket
	 *            socket to add
	 */
	protected void addSocketToPool(Map<String, Map<SockIO, Long>> pool, String host, SockIO socket) {

		if (pool.containsKey(host)) {
			Map<SockIO, Long> sockets = pool.get(host);

			if (sockets != null) {
				sockets.put(socket, new Long(System.currentTimeMillis()));
				return;
			}
		}

		Map<SockIO, Long> sockets = new IdentityHashMap<SockIO, Long>();

		sockets.put(socket, new Long(System.currentTimeMillis()));
		pool.put(host, sockets);
	}

	/**
	 * Removes a socket from specified pool for host. THIS METHOD IS NOT
	 * THREADSAFE, SO BE CAREFUL WHEN USING!
	 * 
	 * Internal utility method.
	 * 
	 * @param pool
	 *            pool to remove from
	 * @param host
	 *            host pool
	 * @param socket
	 *            socket to remove
	 */
	protected void removeSocketFromPool(Map<String, Map<SockIO, Long>> pool, String host, SockIO socket) {
		if (pool.containsKey(host)) {
			Map<SockIO, Long> sockets = pool.get(host);
			if (sockets != null)
				sockets.remove(socket);
		}
	}

	/**
	 * Closes and removes all sockets from specified pool for host. THIS METHOD
	 * IS NOT THREADSAFE, SO BE CAREFUL WHEN USING!
	 * 
	 * Internal utility method.
	 * 
	 * @param pool
	 *            pool to clear
	 * @param host
	 *            host to clear
	 */
	protected void clearHostFromPool(Map<String, Map<SockIO, Long>> pool, String host) {

		if (pool.containsKey(host)) {
			Map<SockIO, Long> sockets = pool.get(host);

			if (sockets != null && sockets.size() > 0) {
				for (Iterator<SockIO> i = sockets.keySet().iterator(); i.hasNext();) {
					SockIO socket = i.next();
					try {
						socket.trueClose();
					} catch (IOException ioe) {
						log.error("++++ failed to close socket: " + ioe.getMessage());
					}

					i.remove();
					socket = null;
				}
			}
		}
	}
}
