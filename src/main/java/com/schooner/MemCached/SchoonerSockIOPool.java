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
package com.schooner.MemCached;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
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
 * static {
 * 	String[] serverlist = { &quot;cache0.server.com:12345&quot;, &quot;cache1.server.com:12345&quot; };
 * 
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
 * 	pool.setInitConn(5);
 * 	pool.setMinConn(5);
 * 	pool.setMaxConn(10);
 * 	pool.setMaintSleep(0);
 * 	pool.setNagle(false);
 * 	pool.initialize();
 * }
 * </pre>
 * 
 * @author Xingen Wang
 * @since 2.5.0
 * @see SchoonerSockIOPool
 */
public class SchoonerSockIOPool {
	// logger
	private static Logger log = LoggerFactory.getLogger(SchoonerSockIOPool.class);

	// store instances of pools
	private static ConcurrentMap<String, SchoonerSockIOPool> pools = new ConcurrentHashMap<String, SchoonerSockIOPool>();

	// avoid recurring construction
	private static ThreadLocal<MessageDigest> MD5 = new ThreadLocal<MessageDigest>() {
		@Override
		protected final MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				if (log.isErrorEnabled())
					log.error("++++ no md5 algorithm found");
				throw new IllegalStateException("++++ no md5 algorythm found");
			}
		}
	};

	public static final int NATIVE_HASH = 0; // native String.hashCode();
	public static final int OLD_COMPAT_HASH = 1; // original compatibility
	// hashing algorithm (works with other clients)
	public static final int NEW_COMPAT_HASH = 2; // new CRC32 based
	// compatibility hashing algorithm (works with other clients)
	public static final int CONSISTENT_HASH = 3; // MD5 Based -- Stops
	// thrashing when a server added or removed
	public static final long MAX_RETRY_DELAY = 10 * 60 * 1000;
	// max of 10 minute delay for fall off

	boolean initialized = false;

	private int minConn = 8;
	private int maxConn = 32;
	private long maxBusyTime = 1000 * 30; // max idle time for avail sockets
	private long maintSleep = 1000 * 30; // maintenance thread sleep time
	private int socketTO = 1000 * 30; // default timeout of socket reads
	private int socketConnectTO = 1000 * 3; // default timeout of socket
	// connections
	@SuppressWarnings("unused")
	private static int recBufferSize = 128;// bufsize

	private long maxWait = 1000; // max wait time for avail sockets
	private int maxIdle = maxConn;
	private int minIdle = GenericObjectPool.DEFAULT_MIN_IDLE;
	private boolean testOnBorrow = GenericObjectPool.DEFAULT_TEST_ON_BORROW;
	private boolean testOnReturn = GenericObjectPool.DEFAULT_TEST_ON_RETURN;
	private long timeBetweenEvictionRunsMillis = GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
	private int numTestsPerEvictionRun = GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
	private long minEvictableIdleTimeMillis = GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
	private boolean testWhileIdle = true;
	private long softMinEvictableIdleTimeMillis = GenericObjectPool.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
	private boolean lifo = GenericObjectPool.DEFAULT_LIFO;

	private boolean aliveCheck = false; // default to not check each connection
	// for being alive
	private boolean failover = true; // default to failover in event of cache
	private boolean failback = true; // only used if failover is also set ...
	// controls putting a dead server back
	// into rotation

	// server dead
	// controls putting a dead server back
	// into rotation
	private boolean nagle = false; // enable/disable Nagle's algorithm
	private int hashingAlg = NATIVE_HASH; // default to using the native hash
	// as it is the fastest

	// locks
	private final ReentrantLock initDeadLock = new ReentrantLock();

	// list of all servers
	private String[] servers;
	private Integer[] weights;
	private Integer totalWeight = 0;

	private List<String> buckets;
	private TreeMap<Long, String> consistentBuckets;

	// map to hold all available sockets
	Map<String, GenericObjectPool> socketPool;

	ConcurrentMap<String, Date> hostDead;

	ConcurrentMap<String, Long> hostDeadDur;

	private AuthInfo authInfo;

	private boolean isTcp;

	private int bufferSize = 1024 * 1025;

	protected SchoonerSockIOPool(boolean isTcp) {
		this.isTcp = isTcp;
	}

	/**
	 * Factory to create/retrieve new pools given a unique poolName.
	 * 
	 * @param poolName
	 *            unique name of the pool
	 * @return instance of SockIOPool
	 */
	public static SchoonerSockIOPool getInstance(String poolName) {
		SchoonerSockIOPool pool;

		synchronized (pools) {
			if (!pools.containsKey(poolName)) {
				pool = new SchoonerSockIOPool(true);
				pools.putIfAbsent(poolName, pool);
			}
		}

		return pools.get(poolName);
	}

	public static SchoonerSockIOPool getInstance(String poolName, AuthInfo authInfo) {
		SchoonerSockIOPool pool;

		synchronized (pools) {
			if (!pools.containsKey(poolName)) {
				pool = new SchoonerSockIOPool(true);
				pool.authInfo = authInfo;
				pools.putIfAbsent(poolName, pool);
			}
		}

		return pools.get(poolName);
	}

	public static SchoonerSockIOPool getInstance(String poolName, boolean isTcp) {
		SchoonerSockIOPool pool;

		synchronized (pools) {
			if (!pools.containsKey(poolName)) {
				pool = new SchoonerSockIOPool(isTcp);
				pools.putIfAbsent(poolName, pool);
			} else {
				pool = pools.get(poolName);
				if (pool.isTcp() == isTcp)
					return pool;
				else
					return null;
			}
		}

		return pool;
	}

	/**
	 * Single argument version of factory used for back compat. Simply creates a
	 * pool named "default".
	 * 
	 * @return instance of SockIOPool
	 */
	public static SchoonerSockIOPool getInstance() {
		return getInstance("default", true);
	}

	public static SchoonerSockIOPool getInstance(AuthInfo authInfo) {
		return getInstance("default", authInfo);
	}

	public static SchoonerSockIOPool getInstance(boolean isTcp) {
		return getInstance("default", isTcp);
	}

	/**
	 * Initializes the pool.
	 */
	public void initialize() {
		initDeadLock.lock();
		try {

			// if servers is not set, or it empty, then
			// throw a runtime exception
			if (servers == null || servers.length <= 0) {
				if (log.isErrorEnabled())
					log.error("++++ trying to initialize with no servers");
				throw new IllegalStateException("++++ trying to initialize with no servers");
			}
			// pools
			socketPool = new HashMap<String, GenericObjectPool>(servers.length);
			hostDead = new ConcurrentHashMap<String, Date>();
			hostDeadDur = new ConcurrentHashMap<String, Long>();
			// only create up to maxCreate connections at once

			// initalize our internal hashing structures
			if (this.hashingAlg == CONSISTENT_HASH)
				populateConsistentBuckets();
			else
				populateBuckets();

			// mark pool as initialized
			this.initialized = true;

		} finally {
			initDeadLock.unlock();
		}

	}

	public boolean isTcp() {
		return isTcp;
	}

	private void populateBuckets() {
		// store buckets in tree map
		buckets = new ArrayList<String>();
		for (int i = 0; i < servers.length; i++) {
			if (this.weights != null && this.weights.length > i) {
				for (int k = 0; k < this.weights[i].intValue(); k++) {
					buckets.add(servers[i]);
				}
			} else {
				buckets.add(servers[i]);
			}

			// Create a socket pool for each host
			// Create an object pool to contain our active connections
			GenericObjectPool gop;
			SchoonerSockIOFactory factory;
			if (authInfo != null) {
				factory = new AuthSchoonerSockIOFactory(servers[i], isTcp, bufferSize, socketTO, socketConnectTO,
						nagle, authInfo);
			} else {
				factory = new SchoonerSockIOFactory(servers[i], isTcp, bufferSize, socketTO, socketConnectTO, nagle);
			}
			gop = new GenericObjectPool(factory, maxConn, GenericObjectPool.WHEN_EXHAUSTED_BLOCK, maxWait, maxIdle,
					minIdle, testOnBorrow, testOnReturn, timeBetweenEvictionRunsMillis, numTestsPerEvictionRun,
					minEvictableIdleTimeMillis, testWhileIdle, this.softMinEvictableIdleTimeMillis, this.lifo);
			factory.setSockets(gop);
			socketPool.put(servers[i], gop);
		}
	}

	private void populateConsistentBuckets() {
		// store buckets in tree map
		consistentBuckets = new TreeMap<Long, String>();

		MessageDigest md5 = MD5.get();
		if (this.totalWeight <= 0 && this.weights != null) {
			for (int i = 0; i < this.weights.length; i++)
				this.totalWeight += (this.weights[i] == null) ? 1 : this.weights[i];
		} else if (this.weights == null) {
			this.totalWeight = this.servers.length;
		}

		for (int i = 0; i < servers.length; i++) {
			int thisWeight = 1;
			if (this.weights != null && this.weights[i] != null)
				thisWeight = this.weights[i];

			double factor = Math.floor(((double) (40 * this.servers.length * thisWeight)) / (double) this.totalWeight);

			for (long j = 0; j < factor; j++) {
				byte[] d = md5.digest((servers[i] + "-" + j).getBytes());
				for (int h = 0; h < 4; h++) {
					Long k = ((long) (d[3 + h * 4] & 0xFF) << 24) | ((long) (d[2 + h * 4] & 0xFF) << 16)
							| ((long) (d[1 + h * 4] & 0xFF) << 8) | ((long) (d[0 + h * 4] & 0xFF));

					consistentBuckets.put(k, servers[i]);
				}
			}

			// Create a socket pool for each host
			// Create an object pool to contain our active connections
			GenericObjectPool gop;
			SchoonerSockIOFactory factory;
			if (authInfo != null) {
				factory = new AuthSchoonerSockIOFactory(servers[i], isTcp, bufferSize, socketTO, socketConnectTO,
						nagle, authInfo);
			} else {
				factory = new SchoonerSockIOFactory(servers[i], isTcp, bufferSize, socketTO, socketConnectTO, nagle);
			}

			gop = new GenericObjectPool(factory, maxConn, GenericObjectPool.WHEN_EXHAUSTED_BLOCK, maxWait, maxIdle,
					minIdle, testOnBorrow, testOnReturn, timeBetweenEvictionRunsMillis, numTestsPerEvictionRun,
					minEvictableIdleTimeMillis, testWhileIdle, this.softMinEvictableIdleTimeMillis, this.lifo);
			factory.setSockets(gop);
			socketPool.put(servers[i], gop);
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
	protected void clearHostFromPool(String host) {
		GenericObjectPool pool = socketPool.get(host);
		pool.clear();
	}

	/**
	 * Gets the host that a particular key / hashcode resides on.
	 * 
	 * @param key
	 * @return
	 */
	public final String getHost(String key) {
		return getHost(key, null);
	}

	/**
	 * Gets the host that a particular key / hashcode resides on.
	 * 
	 * @param key
	 * @param hashcode
	 * @return
	 */
	public final String getHost(String key, Integer hashcode) {
		SchoonerSockIO socket = getSock(key, hashcode);
		String host = socket.getHost();
		socket.close();
		return host;
	}

	/**
	 * Returns appropriate SockIO object given string cache key.
	 * 
	 * @param key
	 *            hashcode for cache key
	 * @return SockIO obj connected to server
	 */
	public final SchoonerSockIO getSock(String key) {
		return getSock(key, null);
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
	public final SchoonerSockIO getSock(String key, Integer hashCode) {

		if (!this.initialized) {
			if (log.isErrorEnabled())
				log.error("attempting to get SockIO from uninitialized pool!");
			return null;
		}

		// if no servers return null
		int size = 0;
		if ((this.hashingAlg == CONSISTENT_HASH && consistentBuckets.size() == 0)
				|| (buckets != null && (size = buckets.size()) == 0))
			return null;
		else if (size == 1) {
			SchoonerSockIO sock = (this.hashingAlg == CONSISTENT_HASH) ? getConnection(consistentBuckets
					.get(consistentBuckets.firstKey())) : getConnection(buckets.get(0));

			return sock;
		}

		// from here on, we are working w/ multiple servers
		// keep trying different servers until we find one
		// making sure we only try each server one time
		Set<String> tryServers = new HashSet<String>(Arrays.asList(servers));
		// get initial bucket
		long bucket = getBucket(key, hashCode);
		String server = (this.hashingAlg == CONSISTENT_HASH) ? consistentBuckets.get(bucket) : buckets
				.get((int) bucket);
		while (!tryServers.isEmpty()) {
			// try to get socket from bucket
			SchoonerSockIO sock = getConnection(server);
			if (sock != null)
				return sock;

			// if we do not want to failover, then bail here
			if (!failover)
				return null;
			// log that we tried
			tryServers.remove(server);
			if (tryServers.isEmpty())
				break;
			// if we failed to get a socket from this server
			// then we try again by adding an incrementer to the
			// current key and then rehashing
			int rehashTries = 0;
			while (!tryServers.contains(server)) {
				String newKey = new StringBuffer().append(rehashTries).append(key).toString();
				// String.format( "%s%s", rehashTries, key );
				bucket = getBucket(newKey, null);
				server = (this.hashingAlg == CONSISTENT_HASH) ? consistentBuckets.get(bucket) : buckets
						.get((int) bucket);
				rehashTries++;
			}
		}
		return null;
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
	public final SchoonerSockIO getConnection(String host) {
		if (!this.initialized) {
			if (log.isErrorEnabled())
				log.error("attempting to get SockIO from uninitialized pool!");
			return null;
		}

		if (host == null)
			return null;

		if (!failback && hostDead.containsKey(host) && hostDeadDur.containsKey(host)) {

			Date store = hostDead.get(host);
			long expire = hostDeadDur.get(host).longValue();

			if ((store.getTime() + expire) > System.currentTimeMillis())
				return null;
		}

		// if we have items in the pool then we can return it
		GenericObjectPool sockets = socketPool.get(host);
		SchoonerSockIO socket;
		try {

			socket = (SchoonerSockIO) sockets.borrowObject();
		} catch (Exception e) {
			socket = null;
		}

		if (socket == null) {
			Date now = new Date();
			hostDead.put(host, now);

			long expire = (hostDeadDur.containsKey(host)) ? (((Long) hostDeadDur.get(host)).longValue() * 2) : 1000;

			if (expire > MAX_RETRY_DELAY)
				expire = MAX_RETRY_DELAY;

			hostDeadDur.put(host, new Long(expire));

			// also clear all entries for this host from availPool
			sockets.clear();
		}

		return socket;
	}

	/**
	 * Closes all sockets in the passed in pool.
	 * 
	 * Internal utility method.
	 * 
	 * @param pool
	 *            pool to close
	 */
	protected final void closeSocketPool() {
		for (Iterator<GenericObjectPool> i = socketPool.values().iterator(); i.hasNext();) {
			GenericObjectPool sockets = i.next();
			try {
				sockets.close();
			} catch (Exception e) {
				if (log.isErrorEnabled())
					log.error("++++ failed to close socket pool.");
			}
		}
	}

	/**
	 * Shuts down the pool.
	 * 
	 * Cleanly closes all sockets.<br/>
	 * Stops the maint thread.<br/>
	 * Nulls out all internal maps<br/>
	 */
	public void shutDown() {
		closeSocketPool();

		socketPool.clear();
		socketPool = null;
		buckets = null;
		consistentBuckets = null;
		initialized = false;
	}

	/**
	 * Returns state of pool.
	 * 
	 * @return <CODE>true</CODE> if initialized.
	 */
	public final boolean isInitialized() {
		return initialized;
	}

	/**
	 * Sets the list of all cache servers.
	 * 
	 * @param servers
	 *            String array of servers [host:port]
	 */
	public final void setServers(String[] servers) {
		this.servers = servers;
	}

	/**
	 * Returns the current list of all cache servers.
	 * 
	 * @return String array of servers [host:port]
	 */
	public final String[] getServers() {
		return this.servers;
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
	public final void setWeights(Integer[] weights) {
		this.weights = weights;
	}

	/**
	 * Returns the current list of weights.
	 * 
	 * @return int array of weights
	 */
	public final Integer[] getWeights() {
		return this.weights;
	}

	/**
	 * Sets the initial number of connections per server in the available pool.
	 * 
	 * @param initConn
	 *            int number of connections
	 */
	public final void setInitConn(int initConn) {
		if (initConn < minConn)
			minConn = initConn;
	}

	/**
	 * Returns the current setting for the initial number of connections per
	 * server in the available pool.
	 * 
	 * @return number of connections
	 */
	public final int getInitConn() {
		return this.minConn;
	}

	/**
	 * Sets the max busy time for threads in the busy pool.
	 * 
	 * @param maxBusyTime
	 *            idle time in ms
	 */
	public final void setMaxBusyTime(long maxBusyTime) {
		this.maxBusyTime = maxBusyTime;
	}

	/**
	 * Returns the current max busy setting.
	 * 
	 * @return max busy setting in ms
	 */
	public final long getMaxBusy() {
		return this.maxBusyTime;
	}

	/**
	 * Set the sleep time between runs of the pool maintenance thread. If set to
	 * 0, then the maint thread will not be started.
	 * 
	 * @param maintSleep
	 *            sleep time in ms
	 */
	public void setMaintSleep(long maintSleep) {
		this.maintSleep = maintSleep;
	}

	/**
	 * Returns the current maint thread sleep time.
	 * 
	 * @return sleep time in ms
	 */
	public long getMaintSleep() {
		return this.maintSleep;
	}

	/**
	 * Sets the socket timeout for reads.
	 * 
	 * @param socketTO
	 *            timeout in ms
	 */
	public final void setSocketTO(int socketTO) {
		this.socketTO = socketTO;
	}

	/**
	 * Returns the socket timeout for reads.
	 * 
	 * @return timeout in ms
	 */
	public final int getSocketTO() {
		return this.socketTO;
	}

	/**
	 * Sets the socket timeout for connect.
	 * 
	 * @param socketConnectTO
	 *            timeout in ms
	 */
	public final void setSocketConnectTO(int socketConnectTO) {
		this.socketConnectTO = socketConnectTO;
	}

	/**
	 * Returns the socket timeout for connect.
	 * 
	 * @return timeout in ms
	 */
	public final int getSocketConnectTO() {
		return this.socketConnectTO;
	}

	/**
	 * Sets the max idle time for threads in the available pool.
	 * 
	 * @param maxIdle
	 *            idle time in ms
	 */
	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	/**
	 * Returns the current max idle setting.
	 * 
	 * @return max idle setting in ms
	 */
	public int getMaxIdle() {
		return this.maxIdle;
	}

	public final long getMaxWait() {
		return this.maxWait;
	}

	public final void setMaxWait(long maxWait) {
		this.maxWait = maxWait;
	}

	public final int getMinIdle() {
		return minIdle;
	}

	public final void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public final boolean getTestOnBorrow() {
		return testOnBorrow;
	}

	public final void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public final boolean getTestOnReturn() {
		return testOnReturn;
	}

	public final void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}

	public final long getTimeBetweenEvictionRunsMillis() {
		return this.timeBetweenEvictionRunsMillis;
	}

	public final void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	public final int getNumTestsPerEvictionRun() {
		return this.numTestsPerEvictionRun;
	}

	public final void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	public final long getMinEvictableIdleTimeMillis() {
		return this.minEvictableIdleTimeMillis;
	}

	public final void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}

	public final boolean getTestWhileIdle() {
		return this.testWhileIdle;
	}

	public final void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}

	public final long getSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis) {
		return this.softMinEvictableIdleTimeMillis;
	}

	public final void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis) {
		this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
	}

	public final boolean getLifo() {
		return this.lifo;
	}

	public final void setLifo(boolean lifo) {
		this.lifo = lifo;
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
	public final void setFailover(boolean failover) {
		this.failover = failover;
	}

	/**
	 * Returns current state of failover flag.
	 * 
	 * @return true/false
	 */
	public final boolean getFailover() {
		return this.failover;
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
		this.failback = failback;
	}

	/**
	 * Returns current state of failover flag.
	 * 
	 * @return true/false
	 */
	public boolean getFailback() {
		return this.failback;
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
	public final void setAliveCheck(boolean aliveCheck) {
		this.aliveCheck = aliveCheck;
	}

	/**
	 * Returns the current status of the aliveCheck flag.
	 * 
	 * @return true / false
	 */
	public final boolean getAliveCheck() {
		return this.aliveCheck;
	}

	/**
	 * Sets the Nagle alg flag for the pool.
	 * 
	 * If false, will turn off Nagle's algorithm on all sockets created.
	 * 
	 * @param nagle
	 *            true/false
	 */
	public final void setNagle(boolean nagle) {
		this.nagle = nagle;
	}

	/**
	 * Returns current status of nagle flag
	 * 
	 * @return true/false
	 */
	public final boolean getNagle() {
		return this.nagle;
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
	public final void setHashingAlg(int alg) {
		this.hashingAlg = alg;
	}

	/**
	 * Returns current status of customHash flag
	 * 
	 * @return true/false
	 */
	public final int getHashingAlg() {
		return this.hashingAlg;
	}

	/**
	 * Internal private hashing method.
	 * 
	 * This is the original hashing algorithm from other clients. Found to be
	 * slow and have poor distribution.
	 * 
	 * @param key
	 *            String to hash
	 * @return hashCode for this string using our own hashing algorithm
	 */
	private static long origCompatHashingAlg(String key) {
		long hash = 0;
		char[] cArr = key.toCharArray();

		for (int i = 0; i < cArr.length; ++i) {
			hash = (hash * 33) + cArr[i];
		}

		return hash;
	}

	/**
	 * Internal private hashing method.
	 * 
	 * This is the new hashing algorithm from other clients. Found to be fast
	 * and have very good distribution.
	 * 
	 * UPDATE: This is dog slow under java
	 * 
	 * @param key
	 * @return
	 */
	private static long newCompatHashingAlg(String key) {
		CRC32 checksum = new CRC32();
		checksum.update(key.getBytes());
		long crc = checksum.getValue();
		return (crc >> 16) & 0x7fff;
	}

	/**
	 * Internal private hashing method.
	 * 
	 * MD5 based hash algorithm for use in the consistent hashing approach.
	 * 
	 * @param key
	 * @return
	 */
	private static long md5HashingAlg(String key) {
		MessageDigest md5 = MD5.get();
		md5.reset();
		md5.update(key.getBytes());
		byte[] bKey = md5.digest();
		long res = ((long) (bKey[3] & 0xFF) << 24) | ((long) (bKey[2] & 0xFF) << 16) | ((long) (bKey[1] & 0xFF) << 8)
				| (long) (bKey[0] & 0xFF);
		return res;
	}

	/**
	 * Returns a bucket to check for a given key.
	 * 
	 * @param key
	 *            String key cache is stored under
	 * @return int bucket
	 */
	private final long getHash(String key, Integer hashCode) {

		if (hashCode != null) {
			if (hashingAlg == CONSISTENT_HASH)
				return hashCode.longValue() & 0xffffffffL;
			else
				return hashCode.longValue();
		} else {
			switch (hashingAlg) {
			case NATIVE_HASH:
				return (long) key.hashCode();
			case OLD_COMPAT_HASH:
				return origCompatHashingAlg(key);
			case NEW_COMPAT_HASH:
				return newCompatHashingAlg(key);
			case CONSISTENT_HASH:
				return md5HashingAlg(key);
			default:
				// use the native hash as a default
				hashingAlg = NATIVE_HASH;
				return (long) key.hashCode();
			}
		}
	}

	private final long getBucket(String key, Integer hashCode) {
		long hc = getHash(key, hashCode);

		if (this.hashingAlg == CONSISTENT_HASH) {
			return findPointFor(hc);
		} else {
			long bucket = hc % buckets.size();
			if (bucket < 0)
				bucket *= -1;
			return bucket;
		}
	}

	/**
	 * Gets the first available key equal or above the given one, if none found,
	 * returns the first k in the bucket
	 * 
	 * @param k
	 *            key
	 * @return
	 */
	private final Long findPointFor(Long hv) {
		// this works in java 6, but still want to release support for java5
		// Long k = this.consistentBuckets.ceilingKey( hv );
		// return ( k == null ) ? this.consistentBuckets.firstKey() : k;

		SortedMap<Long, String> tmap = this.consistentBuckets.tailMap(hv);

		return (tmap.isEmpty()) ? this.consistentBuckets.firstKey() : tmap.firstKey();
	}

	public void setMaxConn(int maxConn) {
		this.maxConn = maxConn;
	}

	public int getMaxConn() {
		return maxConn;
	}

	public void setMinConn(int minConn) {
		this.minConn = minConn;
	}

	public int getMinConn() {
		return minConn;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public static class UDPSockIO extends SchoonerSockIO {

		/**
		 * 
		 * <p>
		 * UDP datagram frame header:
		 * </p>
		 * <p>
		 * +------------+------------+------------+------------+
		 * </p>
		 * <p>
		 * | Request ID | Sequence | Total | Reserved |
		 * </p>
		 * <p>
		 * +------------+------------+------------+------------+
		 * </p>
		 * <p>
		 * | 2bytes | 2bytes | 2bytes | 2bytes |
		 * </p>
		 * <p>
		 * +------------+------------+------------+------------+
		 * </p>
		 */
		public static Short REQUESTID = (short) 0;
		public static final short SEQENCE = (short) 0x0000;
		public static final short TOTAL = (short) 0x0001;
		public static final short RESERVED = (short) 0x0000;

		private static ConcurrentMap<String, byte[]> data = new ConcurrentHashMap<String, byte[]>();

		private class UDPDataItem {

			private short counter = 0;
			private boolean isFinished = false;
			private int length = 0;

			private short total;

			public synchronized short getTotal() {
				return total;
			}

			public synchronized void setTotal(short total) {
				if (this.total == 0)
					this.total = total;
			}

			public synchronized short getCounter() {
				return counter;
			}

			public synchronized short incrCounter() {
				return ++counter;
			}

			public synchronized boolean isFinished() {
				return isFinished;
			}

			public synchronized void setFinished(boolean isFinished) {
				this.isFinished = isFinished;
			}

			public synchronized int getLength() {
				return length;
			}

			public synchronized void addLength(int alength) {
				this.length += alength;
			}
		}

		public static ConcurrentMap<Short, UDPDataItem> dataStore = new ConcurrentHashMap<Short, UDPDataItem>();

		public DatagramChannel channel;

		private Selector selector;

		@Override
		public void trueClose() throws IOException {
			if (selector != null) {
				selector.close();
				channel.close();
			}
		}

		public UDPSockIO(GenericObjectPool sockets, String host, int bufferSize, int timeout) throws IOException,
				UnknownHostException {
			super(sockets, bufferSize);

			String[] ip = host.split(":");
			channel = DatagramChannel.open();
			channel.configureBlocking(false);
			SocketAddress address = new InetSocketAddress(ip[0], Integer.parseInt(ip[1]));
			channel.connect(address);
			channel.socket().setSoTimeout(timeout);
			selector = Selector.open();
			((DatagramChannel) channel).register(selector, SelectionKey.OP_READ);
			writeBuf = ByteBuffer.allocateDirect(bufferSize);
		}

		@Override
		public ByteChannel getByteChannel() {
			return channel;
		}

		@Override
		public short preWrite() {
			writeBuf.clear();
			short rid = 0;
			synchronized (REQUESTID) {
				REQUESTID++;
				rid = REQUESTID.shortValue();
			}
			writeBuf.putShort(rid);
			writeBuf.putShort(SEQENCE);
			writeBuf.putShort(TOTAL);
			writeBuf.putShort(RESERVED);
			return rid;
		}

		@Override
		public byte[] getResponse(short rid) throws IOException {

			long timeout = 1000;
			long timeRemaining = timeout;

			int length = 0;
			byte[] ret = null;
			short requestID;
			short sequence;

			UDPDataItem mItem = new UDPDataItem();
			UDPSockIO.dataStore.put(rid, mItem);

			long startTime = System.currentTimeMillis();
			while (timeRemaining > 0 && !mItem.isFinished()) {
				int n = selector.select(500);
				if (n > 0) {
					// we've got some activity; handle it
					Iterator<SelectionKey> it = selector.selectedKeys().iterator();
					while (it.hasNext()) {
						SelectionKey skey = it.next();
						it.remove();
						if (skey.isReadable()) {
							DatagramChannel sc = (DatagramChannel) skey.channel();
							// every loop handles one datagram.
							do {
								readBuf.clear();
								sc.read(readBuf);
								length = readBuf.position();
								if (length <= 8) {
									break;
								}
								readBuf.flip();
								// get the udpheader of the response.
								requestID = readBuf.getShort();
								UDPDataItem item = UDPSockIO.dataStore.get(requestID);
								if (item != null && !item.isFinished) {
									item.addLength(length - 8);
									sequence = readBuf.getShort();
									item.setTotal(readBuf.getShort());
									readBuf.getShort(); // get reserved

									// save those datagram into a map, so we
									// can change the sequence of them.
									byte[] tmp = new byte[length - 8];
									readBuf.get(tmp);
									item.incrCounter();
									data.put(requestID + "_" + sequence, tmp);
									if (item.getCounter() == item.getTotal()) {
										item.setFinished(true);
									}
								}
							} while (true);
						}
					}

				} else {
					// timeout likely... better check
					// TODO: This seems like a problem area that we need to
					// figure out how to handle.
					// log.error("selector timed out waiting for activity");
					break;
				}

				timeRemaining = timeout - (System.currentTimeMillis() - startTime);
			}

			if (!mItem.isFinished) {
				UDPSockIO.dataStore.remove(rid);
				for (short sq = 0; sq < mItem.getTotal(); sq = (short) (sq + 1)) {
					data.remove(rid + "_" + sq);
				}
				return null;
			}

			// rearrange the datagram's sequence.
			int counter = mItem.getLength();
			ret = new byte[counter];
			counter = 0;
			boolean isOk = true;
			for (short sq = 0; sq < mItem.getTotal(); sq = (short) (sq + 1)) {
				byte[] src = data.remove(rid + "_" + sq);
				if (src == null)
					isOk = false;
				if (isOk) {
					System.arraycopy(src, 0, ret, counter, src.length);
					counter += src.length;
				}
			}
			UDPSockIO.dataStore.remove(rid);
			// selector.close();

			if (!isOk)
				return null;

			return ret;
		}

		@Override
		public void close() {
			readBuf.clear();
			writeBuf.clear();
			try {
				sockets.returnObject(this);
			} catch (Exception e) {
				if (log.isErrorEnabled())
					log.error("++++ error closing socket: " + toString() + " for host: " + getHost());
			}
		}

		public String getHost() {
			return channel.socket().getInetAddress().getHostName();
		}

		@Override
		public void clearEOL() throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public int read(byte[] b) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String readLine() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void trueClose(boolean addToDeadPool) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public SocketChannel getChannel() {
			// TODO Auto-generated method stub
			return null;
		}
	}

	/**
	 * MemCached Java client, utility class for Socket IO.
	 * 
	 * This class is a wrapper around a Socket and its streams.
	 * 
	 * @author greg whalin <greg@meetup.com>
	 * @author Richard 'toast' Russo <russor@msoe.edu>
	 * @version 1.5
	 */
	public static class TCPSockIO extends SchoonerSockIO {

		// logger
		private static Logger log = LoggerFactory.getLogger(SchoonerSockIO.class);
		// data
		private String host;
		private Socket sock;

		public java.nio.channels.SocketChannel sockChannel;

		private int hash = 0;

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
		public TCPSockIO(GenericObjectPool sockets, String host, int bufferSize, int timeout, int connectTimeout,
				boolean noDelay) throws IOException, UnknownHostException {

			super(sockets, bufferSize);

			// allocate a new receive buffer
			String[] ip = host.split(":");

			// get socket: default is to use non-blocking connect
			sock = getSocket(ip[0], Integer.parseInt(ip[1]), connectTimeout);

			writeBuf = ByteBuffer.allocateDirect(bufferSize);

			if (timeout >= 0)
				this.sock.setSoTimeout(timeout);

			// testing only
			sock.setTcpNoDelay(noDelay);

			// wrap streams
			sockChannel = sock.getChannel();
			hash = sock.hashCode();
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
		protected final static Socket getSocket(String host, int port, int timeout) throws IOException {
			SocketChannel sock = SocketChannel.open();
			// fix connection leak with exception, due to elendel's fix.
			try {
				sock.socket().connect(new InetSocketAddress(host, port), timeout);
			} catch (IOException e) {
				sock.socket().close();
				throw e;
			}
			return sock.socket();
		}

		/**
		 * Lets caller get access to underlying channel.
		 * 
		 * @return the backing SocketChannel
		 */
		public final SocketChannel getChannel() {
			return sock.getChannel();
		}

		/**
		 * returns the host this socket is connected to
		 * 
		 * @return String representation of host (hostname:port)
		 */
		public final String getHost() {
			return this.host;
		}

		/**
		 * closes socket and all streams connected to it
		 * 
		 * @throws IOException
		 *             if fails to close streams or socket
		 */
		public final void trueClose() throws IOException {
			readBuf.clear();

			boolean err = false;
			StringBuilder errMsg = new StringBuilder();

			if (sockChannel == null || sock == null) {
				err = true;
				errMsg.append("++++ socket or its streams already null in trueClose call");
			}

			if (sockChannel != null) {
				try {
					sockChannel.close();
				} catch (IOException ioe) {
					if (log.isErrorEnabled()) {
						log.error("++++ error closing input stream for socket: " + toString() + " for host: "
								+ getHost());
						log.error(ioe.getMessage(), ioe);
					}
					errMsg.append("++++ error closing input stream for socket: " + toString() + " for host: "
							+ getHost() + "\n");
					errMsg.append(ioe.getMessage());
					err = true;
				}
			}

			if (sock != null) {
				try {
					sock.close();
				} catch (IOException ioe) {
					if (log.isErrorEnabled()) {
						log.error("++++ error closing socket: " + toString() + " for host: " + getHost());
						log.error(ioe.getMessage(), ioe);
					}
					errMsg.append("++++ error closing socket: " + toString() + " for host: " + getHost() + "\n");
					errMsg.append(ioe.getMessage());
					err = true;
				}
			}

			sockChannel = null;
			sock = null;

			if (err)
				throw new IOException(errMsg.toString());
		}

		/**
		 * sets closed flag and checks in to connection pool but does not close
		 * connections
		 */
		public final void close() {
			readBuf.clear();
			try {
				sockets.returnObject(this);
			} catch (Exception e) {
				if (log.isErrorEnabled())
					log.error("++++ error closing socket: " + toString() + " for host: " + getHost());
			}
		}

		/**
		 * checks if the connection is open
		 * 
		 * @return true if connected
		 */
		public boolean isConnected() {
			return (sock != null && sock.isConnected());
		}

		/**
		 * read fix length data from server and store it in the readBuf
		 * 
		 * @param length
		 *            data length
		 * @throws IOException
		 *             if an io error happens
		 */
		public final void readBytes(int length) throws IOException {
			if (sock == null || !sock.isConnected()) {
				if (log.isErrorEnabled())
					log.error("++++ attempting to read from closed socket");
				throw new IOException("++++ attempting to read from closed socket");
			}

			int readCount;
			while (length > 0) {
				readCount = sockChannel.read(readBuf);
				length -= readCount;
			}
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
				if (log.isErrorEnabled())
					log.error("++++ attempting to write to closed socket");
				throw new IOException("++++ attempting to write to closed socket");
			}
			sockChannel.write(ByteBuffer.wrap(b));
		}

		/**
		 * writes data stored in writeBuf to server
		 * 
		 * @throws IOException
		 *             if an io error happens
		 */
		@Override
		public void flush() throws IOException {
			writeBuf.flip();
			sockChannel.write(writeBuf);
		}

		/**
		 * use the sockets hashcode for this object so we can key off of SockIOs
		 * 
		 * @return int hashcode
		 */
		public final int hashCode() {
			return (sock == null) ? 0 : hash;
		}

		/**
		 * returns the string representation of this socket
		 * 
		 * @return string
		 */
		public final String toString() {
			return (sock == null) ? "" : sock.toString();
		}

		/**
		 * Hack to reap any leaking children.
		 */
		protected final void finalize() throws Throwable {
			try {
				if (sock != null) {
					// log.error("++++ closing potentially leaked socket in finalize");
					sock.close();
					sock = null;
				}
			} catch (Throwable t) {
				log.error(t.getMessage(), t);

			} finally {
				super.finalize();
			}
		}

		@Override
		public short preWrite() {
			// should do nothing, this method is for UDP only.
			return 0;
		}

		@Override
		public byte[] getResponse(short rid) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void clearEOL() throws IOException {
			if (sock == null || !sock.isConnected()) {
				if (log.isErrorEnabled())
					log.error("++++ attempting to read from closed socket");
				throw new IOException("++++ attempting to read from closed socket");
			}

			byte[] b = new byte[1];
			boolean eol = false;
			InputStream in = sock.getInputStream();
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

		@Override
		public int read(byte[] b) throws IOException {
			if (sock == null || !sock.isConnected()) {
				if (log.isErrorEnabled())
					log.error("++++ attempting to read from closed socket");
				throw new IOException("++++ attempting to read from closed socket");
			}

			int count = 0;
			InputStream in = sock.getInputStream();
			while (count < b.length) {
				int cnt = in.read(b, count, (b.length - count));
				count += cnt;
			}

			return count;
		}

		@Override
		public String readLine() throws IOException {
			if (sock == null || !sock.isConnected()) {
				if (log.isErrorEnabled())
					log.error("++++ attempting to read from closed socket");
				throw new IOException("++++ attempting to read from closed socket");
			}

			byte[] b = new byte[1];
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			boolean eol = false;
			InputStream in = sock.getInputStream();
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

		@Override
		public void trueClose(boolean addToDeadPool) throws IOException {
			trueClose();
		}

		@Override
		public ByteChannel getByteChannel() {
			// TODO Auto-generated method stub
			return sockChannel;
		}
	}

}