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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.danga.MemCached.ErrorHandler;
import com.danga.MemCached.MemCachedClient;
import com.schooner.MemCached.command.DeletionCommand;
import com.schooner.MemCached.command.FlushAllCommand;
import com.schooner.MemCached.command.IncrdecrCommand;
import com.schooner.MemCached.command.RetrievalCommand;
import com.schooner.MemCached.command.StatsCommand;
import com.schooner.MemCached.command.StorageCommand;
import com.schooner.MemCached.command.SyncAllCommand;
import com.schooner.MemCached.command.SyncCommand;

/**
 * This client implements the text protocol of memcached in a very high
 * performance way.<br>
 * Please use the wrapper class {@link MemCachedClient2} for accessing the
 * memcached server.
 * 
 * @author Xingen Wang
 * @since 2.5.0
 * @see BinaryClient
 */
public class AscIIUDPClient extends MemCachedClient {
	private TransCoder transCoder = new ObjectTransCoder();

	// pool instance
	private SchoonerSockIOPool pool;

	// which pool to use
	private String poolName;

	// flags
	private boolean sanitizeKeys;
	@SuppressWarnings("unused")
	private boolean primitiveAsString;
	@SuppressWarnings("unused")
	private boolean compressEnable;
	@SuppressWarnings("unused")
	private long compressThreshold;
	@SuppressWarnings("unused")
	private String defaultEncoding = "utf-8";

	public static final byte B_DELIMITER = 32;
	public static final byte B_RETURN = (byte) 13;

	public boolean isUseBinaryProtocol() {
		return false;
	}

	/**
	 * Creates a new instance of MemCachedClient.
	 */
	public AscIIUDPClient() {
		this("default");
	}

	/**
	 * Creates a new instance of MemCachedClient accepting a passed in pool
	 * name.
	 * 
	 * @param poolName
	 *            name of SockIOPool
	 * @param binaryProtocal
	 *            whether use binary protocol.
	 */
	public AscIIUDPClient(String poolName) {
		this.poolName = poolName;
		init();
	}

	public AscIIUDPClient(String poolName, ClassLoader cl, ErrorHandler eh) {
		this.poolName = poolName;
		this.classLoader = cl;
		this.errorHandler = eh;
		init();
	}

	/**
	 * Initializes client object to defaults.
	 * 
	 * This enables compression and sets compression threshhold to 15 KB.
	 */
	private void init() {
		this.sanitizeKeys = true;
		this.primitiveAsString = false;
		this.compressEnable = true;
		this.compressThreshold = COMPRESS_THRESH;
		this.defaultEncoding = "UTF-8";
		this.poolName = (this.poolName == null) ? "default" : this.poolName;

		// get a pool instance to work with for the life of this instance
		this.pool = SchoonerSockIOPool.getInstance(poolName);
	}

	public boolean set(String key, Object value) {
		return set("set", key, value, null, null, 0L);
	}

	public boolean set(String key, Object value, Integer hashCode) {
		return set("set", key, value, null, hashCode, 0L);
	}

	public boolean set(String key, Object value, Date expiry) {
		return set("set", key, value, expiry, null, 0L);
	}

	public boolean set(String key, Object value, Date expiry, Integer hashCode) {
		return set("set", key, value, expiry, hashCode, 0L);
	}

	public boolean add(String key, Object value) {
		return set("add", key, value, null, null, 0L);
	}

	public boolean add(String key, Object value, Integer hashCode) {
		return set("add", key, value, null, hashCode, 0L);
	}

	public boolean add(String key, Object value, Date expiry) {
		return set("add", key, value, expiry, null, 0L);
	}

	public boolean add(String key, Object value, Date expiry, Integer hashCode) {
		return set("add", key, value, expiry, hashCode, 0L);
	}

	public boolean append(String key, Object value, Integer hashCode) {
		return set("append", key, value, null, hashCode, 0L);
	}

	public boolean append(String key, Object value) {
		return set("append", key, value, null, null, 0L);
	}

	public boolean cas(String key, Object value, Integer hashCode, long casUnique) {
		return set("cas", key, value, null, hashCode, casUnique);
	}

	public boolean cas(String key, Object value, Date expiry, long casUnique) {
		return set("cas", key, value, expiry, null, casUnique);
	}

	public boolean cas(String key, Object value, Date expiry, Integer hashCode, long casUnique) {
		return set("cas", key, value, expiry, hashCode, casUnique);
	}

	public boolean cas(String key, Object value, long casUnique) {
		return set("cas", key, value, null, null, casUnique);
	}

	public boolean prepend(String key, Object value, Integer hashCode) {
		return set("prepend", key, value, null, hashCode, 0L);
	}

	public boolean prepend(String key, Object value) {
		return set("prepend", key, value, null, null, 0L);
	}

	public boolean replace(String key, Object value) {
		return set("replace", key, value, null, null, 0L);
	}

	public boolean replace(String key, Object value, Integer hashCode) {
		return set("replace", key, value, null, hashCode, 0L);
	}

	public boolean replace(String key, Object value, Date expiry) {
		return set("replace", key, value, expiry, null, 0L);
	}

	public boolean replace(String key, Object value, Date expiry, Integer hashCode) {
		return set("replace", key, value, expiry, hashCode, 0L);
	}

	/**
	 * Stores data to cache.
	 * 
	 * If data does not already exist for this key on the server, or if the key
	 * is being<br/>
	 * deleted, the specified value will not be stored.<br/>
	 * The server will automatically delete the value when the expiration time
	 * has been reached.<br/>
	 * <br/>
	 * If compression is enabled, and the data is longer than the compression
	 * threshold<br/>
	 * the data will be stored in compressed form.<br/>
	 * <br/>
	 * As of the current release, all objects stored will use java
	 * serialization.
	 * 
	 * @param cmdname
	 *            action to take (set, add, replace)
	 * @param key
	 *            key to store cache under
	 * @param value
	 *            object to cache
	 * @param expiry
	 *            expiration
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true/false indicating success
	 */
	private boolean set(String cmdname, String key, Object value, Date expiry, Integer hashCode, Long casUnique) {

		if (cmdname == null || key == null) {
			log.error("key is null or cmd is null/empty for set()");
			return false;
		}

		try {
			key = sanitizeKey(key);
		} catch (UnsupportedEncodingException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnSet(this, e, key);

			log.error("failed to sanitize your key!", e);
			return false;
		}

		if (value == null) {
			log.error("trying to store a null value to cache");
			return false;
		}

		// get SockIO obj
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		if (sock == null) {
			if (errorHandler != null)
				errorHandler.handleErrorOnSet(this, new IOException("no socket to server available"), key);
			return false;
		}

		if (expiry == null)
			expiry = new Date(0);

		try {
			StorageCommand setCmd = new StorageCommand(cmdname, key, value, expiry, hashCode, casUnique, transCoder);
			short rid = setCmd.request(sock);
			return setCmd.response(sock, rid);
		} catch (IOException e) {
			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnSet(this, e, key);

			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on set");
			log.error(e.getMessage(), e);

			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error(new StringBuffer().append("++++ failed to close socket : ").append(sock.toString())
						.toString());
			}
			sock = null;
		} finally {
			if (sock != null) {
				sock.close();
				sock = null;
			}
		}
		return false;
	}

	/**
	 * set transcoder. TransCoder is used to customize the serialization and
	 * deserialization.
	 * 
	 * @param transCoder
	 */
	public void setTransCoder(TransCoder transCoder) {
		this.transCoder = transCoder;
	}

	public long addOrDecr(String key) {
		return addOrDecr(key, 0, null);
	}

	public long addOrDecr(String key, long inc) {
		return addOrDecr(key, inc, null);
	}

	public long addOrDecr(String key, long inc, Integer hashCode) {
		boolean ret = add(key, "" + inc, hashCode);
		if (ret) {
			return inc;
		} else {
			return incrdecr("decr", key, inc, hashCode);
		}
	}

	public long addOrIncr(String key) {
		return addOrIncr(key, 0, null);
	}

	public long addOrIncr(String key, long inc) {
		return addOrIncr(key, inc, null);
	}

	public long addOrIncr(String key, long inc, Integer hashCode) {
		boolean ret = add(key, "" + inc, hashCode);

		if (ret) {
			return inc;
		} else {
			return incrdecr("incr", key, inc, hashCode);
		}
	}

	public long decr(String key) {
		return incrdecr("decr", key, 1, null);
	}

	public long decr(String key, long inc) {
		return incrdecr("decr", key, inc, null);
	}

	public long decr(String key, long inc, Integer hashCode) {
		return incrdecr("decr", key, inc, hashCode);
	}

	public boolean delete(String key) {
		return delete(key, null, null);
	}

	public boolean delete(String key, Date expiry) {
		return delete(key, null, expiry);
	}

	public boolean flushAll() {
		return flushAll(null);
	}

	public boolean flushAll(String[] servers) {
		// get SockIOPool instance
		// return false if unable to get SockIO obj
		if (pool == null) {
			log.error("++++ unable to get SockIOPool instance");
			return false;
		}

		// get all servers and iterate over them
		servers = (servers == null) ? pool.getServers() : servers;

		// if no servers, then return early
		if (servers == null || servers.length <= 0) {
			log.error("++++ no servers to flush");
			return false;
		}

		boolean success = true;

		for (int i = 0; i < servers.length; i++) {

			SchoonerSockIO sock = pool.getConnection(servers[i]);
			if (sock == null) {
				if (errorHandler != null)
					errorHandler.handleErrorOnFlush(this, new IOException("no socket to server available"));

				log.error("++++ unable to get connection to : " + servers[i]);
				success = false;
				continue;
			}

			try {
				FlushAllCommand flushallCmd = new FlushAllCommand();
				short rid = flushallCmd.request(sock);
				success = flushallCmd.response(sock, rid);

				if (!success) {
					return success;
				}
			} catch (IOException e) {
				// if we have an errorHandler, use its hook
				if (errorHandler != null)
					errorHandler.handleErrorOnFlush(this, e);

				// exception thrown
				log.error("++++ exception thrown while writing bytes to server on flushAll");
				log.error(e.getMessage(), e);

				try {
					sock.trueClose();
				} catch (IOException ioe) {
					log.error("++++ failed to close socket : " + sock.toString());
				}

				success = false;
				sock = null;
			} finally {
				if (sock != null) {
					sock.close();
					sock = null;
				}
			}
		}

		return success;
	}

	public Object get(String key) {
		return get(key, null);
	}

	public Object get(String key, Integer hashCode) {
		return get("get", key, hashCode).value;
	}

	public Map<String, Object> getMulti(String[] keys) {
		return getMulti(keys, null);
	}

	public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes) {

		if (keys == null || keys.length == 0) {
			log.error("missing keys for getMulti()");
			return null;
		}

		Map<String, Object> ret = new HashMap<String, Object>(keys.length);

		for (int i = 0; i < keys.length; ++i) {

			String key = keys[i];
			if (key == null) {
				log.error("null key, so skipping");
				continue;
			}

			Integer hash = null;
			if (hashCodes != null && hashCodes.length > i)
				hash = hashCodes[i];

			// get SockIO obj from cache key
			SchoonerSockIO sock = pool.getSock(key, hash);

			if (sock == null) {
				if (errorHandler != null)
					errorHandler.handleErrorOnGet(this, new IOException("no socket to server available"), key);
				continue;
			}

			ret.put(key, get("get", key, hash).value);
			sock.close();
		}
		return ret;
	}

	public Object[] getMultiArray(String[] keys) {
		return getMultiArray(keys, null);
	}

	public Object[] getMultiArray(String[] keys, Integer[] hashCodes) {

		Map<String, Object> data = getMulti(keys, hashCodes);

		if (data == null)
			return null;

		Object[] res = new Object[keys.length];
		for (int i = 0; i < keys.length; i++) {
			res[i] = data.get(keys[i]);
		}

		return res;
	}

	/**
	 * Retrieve multiple objects from the memcache.
	 * 
	 * This is recommended over repeated calls to {@link #get(String) get()},
	 * since it<br/>
	 * is more efficient.<br/>
	 * 
	 * @param keys
	 *            String array of keys to retrieve
	 * @param hashCodes
	 *            if not null, then the Integer array of hashCodes
	 * @param asString
	 *            if true, retrieve string vals
	 * @return Object array ordered in same order as key array containing
	 *         results
	 */
	public Object[] getMultiArray(String[] keys, Integer[] hashCodes, boolean asString) {

		Map<String, Object> data = getMulti(keys, hashCodes, asString);

		if (data == null)
			return null;

		Object[] res = new Object[keys.length];
		for (int i = 0; i < keys.length; i++) {
			res[i] = data.get(keys[i]);
		}

		return res;
	}

	public MemcachedItem gets(String key) {
		return gets(key, null);
	}

	public MemcachedItem gets(String key, Integer hashCode) {
		return get("gets", key, hashCode);
	}

	/**
	 * get memcached item from server.
	 * 
	 * @param cmd
	 *            cmd to be used, get/gets
	 * @param key
	 *            specified key
	 * @param hashCode
	 *            specified hashcode
	 * @return memcached item with value in it.
	 */
	private MemcachedItem get(String cmd, String key, Integer hashCode) {
		MemcachedItem item = new MemcachedItem();

		if (key == null) {
			log.error("key is null for get()");
			return item;
		}

		try {
			key = sanitizeKey(key);
		} catch (UnsupportedEncodingException e) {
			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, e, key);

			log.error("failed to sanitize your key!", e);
			return null;
		}

		// get SockIO obj using cache key
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		if (sock == null) {
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, new IOException("no socket to server available"), key);
			return item;
		}

		RetrievalCommand retrieval = new RetrievalCommand(cmd, key);
		try {
			short rid = retrieval.request(sock);
			return retrieval.response(sock, transCoder, rid);
		} catch (IOException e) {
			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error(new StringBuffer().append("++++ failed to close socket : ").append(sock.toString())
						.toString());
			}
			sock = null;
		} finally {
			if (sock != null) {
				sock.close();
				sock = null;
			}
		}
		return item;

	}

	public long incr(String key) {
		return incrdecr("incr", key, 1, null);
	}

	public long incr(String key, long inc) {
		return incrdecr("incr", key, inc, null);
	}

	public long incr(String key, long inc, Integer hashCode) {
		return incrdecr("incr", key, inc, hashCode);
	}

	public boolean keyExists(String key) {
		return (this.get(key, null) != null);
	}

	public Map<String, Map<String, String>> stats() {
		return stats(null);
	}

	public Map<String, Map<String, String>> stats(String[] servers) {
		return stats(servers, "stats\r\n", STATS);
	}

	public Map<String, Map<String, String>> statsCacheDump(int slabNumber, int limit) {
		return statsCacheDump(null, slabNumber, limit);
	}

	public Map<String, Map<String, String>> statsCacheDump(String[] servers, int slabNumber, int limit) {
		return stats(servers, String.format("stats cachedump %d %d\r\n", slabNumber, limit), ITEM);
	}

	public Map<String, Map<String, String>> statsItems() {
		return statsItems(null);
	}

	public Map<String, Map<String, String>> statsItems(String[] servers) {
		return stats(servers, "stats items\r\n", STATS);
	}

	public Map<String, Map<String, String>> statsSlabs() {
		return statsSlabs(null);
	}

	public Map<String, Map<String, String>> statsSlabs(String[] servers) {
		return stats(servers, "stats slabs\r\n", STATS);
	}

	public boolean sync(String key, Integer hashCode) {
		if (key == null) {
			log.error("null value for key passed to delete()");
			return false;
		}

		// get SockIO obj from hash or from key
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		// return false if unable to get SockIO obj
		if (sock == null) {
			return false;
		}

		try {
			SyncCommand syncCmd = new SyncCommand(key, hashCode);
			short rid = syncCmd.request(sock);
			return syncCmd.response(sock, rid);
		} catch (IOException e) {
			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on delete");
			log.error(e.getMessage(), e);

			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error(new StringBuffer().append("++++ failed to close socket : ").append(sock.toString())
						.toString());
			}

			sock = null;
		} finally {
			if (sock != null) {
				sock.close();
				sock = null;
			}
		}

		return false;
	}

	public boolean sync(String key) {
		return sync(key, null);
	}

	public boolean syncAll() {
		return syncAll(null);
	}

	public boolean syncAll(String[] servers) {
		// get SockIOPool instance
		// return false if unable to get SockIO obj
		if (pool == null) {
			log.error("++++ unable to get SockIOPool instance");
			return false;
		}

		// get all servers and iterate over them
		servers = (servers == null) ? pool.getServers() : servers;

		// if no servers, then return early
		if (servers == null || servers.length <= 0) {
			log.error("++++ no servers to sync");
			return false;
		}

		boolean success = true;

		for (int i = 0; i < servers.length; i++) {

			SchoonerSockIO sock = pool.getConnection(servers[i]);
			if (sock == null) {
				log.error("++++ unable to get connection to : " + servers[i]);
				success = false;
				continue;
			}

			try {
				SyncAllCommand syncCmd = new SyncAllCommand();
				short rid = syncCmd.request(sock);
				success = syncCmd.response(sock, rid);

				if (!success)
					return false;
			} catch (IOException e) {
				// exception thrown
				log.error("++++ exception thrown while writing bytes to server on flushAll");
				log.error(e.getMessage(), e);

				try {
					sock.trueClose();
				} catch (IOException ioe) {
					log.error("++++ failed to close socket : " + sock.toString());
				}

				success = false;
				sock = null;
			} finally {
				if (sock != null) {
					sock.close();
					sock = null;
				}
			}
		}

		return success;
	}

	public boolean delete(String key, Integer hashCode, Date expiry) {

		if (key == null) {
			log.error("null value for key passed to delete()");
			return false;
		}

		// get SockIO obj from hash or from key
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		// return false if unable to get SockIO obj
		if (sock == null) {
			if (errorHandler != null)
				errorHandler.handleErrorOnDelete(this, new IOException("no socket to server available"), key);
			return false;
		}

		try {
			DeletionCommand deletion = new DeletionCommand(key, hashCode, expiry);
			short rid = deletion.request(sock);
			return deletion.response(sock, rid);
		} catch (IOException e) {
			if (errorHandler != null)
				errorHandler.handleErrorOnDelete(this, e, key);

			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on delete");
			log.error(e.getMessage(), e);
			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error(new StringBuffer().append("++++ failed to close socket : ").append(sock.toString())
						.toString());
			}
			sock = null;
		} finally {
			if (sock != null) {
				sock.close();
				sock = null;
			}
		}
		return false;
	}

	/**
	 * Increments/decrements the value at the specified key by inc.
	 * 
	 * Note that the server uses a 32-bit unsigned integer, and checks for<br/>
	 * underflow. In the event of underflow, the result will be zero. Because<br/>
	 * Java lacks unsigned types, the value is returned as a 64-bit integer.<br/>
	 * The server will only decrement a value if it already exists;<br/>
	 * if a value is not found, -1 will be returned.
	 * 
	 * @param cmdname
	 *            increment/decrement
	 * @param key
	 *            cache key
	 * @param inc
	 *            amount to incr or decr
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return new value or -1 if not exist
	 */
	private long incrdecr(String cmdname, String key, long inc, Integer hashCode) {

		if (key == null) {
			log.error("null key for incrdecr()");
			return -1;
		}

		try {
			key = sanitizeKey(key);
		} catch (UnsupportedEncodingException e) {
			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, e, key);

			log.error("failed to sanitize your key!", e);
			return -1;
		}

		// get SockIO obj for given cache key
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		if (sock == null) {
			if (errorHandler != null)
				errorHandler.handleErrorOnSet(this, new IOException("no socket to server available"), key);
			return -1;
		}

		try {
			IncrdecrCommand idCmd = new IncrdecrCommand(cmdname, key, inc, hashCode);
			short rid = idCmd.request(sock);
			if (idCmd.response(sock, rid)) {
				return idCmd.getResult();
			} else {
				return -1;
			}
		} catch (IOException e) {
			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, e, key);

			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on incr/decr");
			log.error(e.getMessage(), e);

			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error(new StringBuffer().append("++++ failed to close socket : ").append(sock.toString())
						.toString());
			}

			sock = null;
		} finally {
			if (sock != null) {
				sock.close();
				sock = null;
			}
		}

		return -1;
	}

	private Map<String, Map<String, String>> stats(String[] servers, String command, String lineStart) {

		if (command == null || command.trim().equals("")) {
			log.error("++++ invalid / missing command for stats()");
			return null;
		}

		// get all servers and iterate over them
		servers = (servers == null) ? pool.getServers() : servers;

		// if no servers, then return early
		if (servers == null || servers.length <= 0) {
			log.error("++++ no servers to check stats");
			return null;
		}

		// array of stats Maps
		Map<String, Map<String, String>> statsMaps = new HashMap<String, Map<String, String>>();

		for (int i = 0; i < servers.length; i++) {

			SchoonerSockIO sock = pool.getConnection(servers[i]);
			if (sock == null) {
				if (errorHandler != null)
					errorHandler.handleErrorOnStats(this, new IOException("no socket to server available"));
				continue;
			}
			// build command
			try {
				StatsCommand statsCmd = new StatsCommand(command, lineStart);
				short rid = statsCmd.request(sock);
				Map<String, String> stats = statsCmd.response(sock, rid);
				statsMaps.put(servers[i], stats);
			} catch (IOException e) {
				// if we have an errorHandler, use its hook
				if (errorHandler != null)
					errorHandler.handleErrorOnStats(this, e);

				// exception thrown
				log.error("++++ exception thrown while writing bytes to server on stats");
				log.error(e.getMessage(), e);

				try {
					sock.trueClose();
				} catch (IOException ioe) {
					log.error("++++ failed to close socket : " + sock.toString());
				}

				sock = null;
			} finally {
				if (sock != null) {
					sock.close();
					sock = null;
				}
			}
		}

		return statsMaps;
	}

	@Override
	public void setCompressEnable(boolean compressEnable) {
		this.compressEnable = compressEnable;
	}

	@Override
	public void setCompressThreshold(long compressThreshold) {
		this.compressThreshold = compressThreshold;
	}

	@Override
	public void setDefaultEncoding(String defaultEncoding) {
		this.defaultEncoding = defaultEncoding;
	}

	@Override
	public void setPrimitiveAsString(boolean primitiveAsString) {
		this.primitiveAsString = primitiveAsString;
	}

	@Override
	public void setSanitizeKeys(boolean sanitizeKeys) {
		this.sanitizeKeys = sanitizeKeys;
	}

	private String sanitizeKey(String key) throws UnsupportedEncodingException {
		return (sanitizeKeys) ? URLEncoder.encode(key, "UTF-8") : key;
	}

	@Override
	public Object get(String key, Integer hashCode, boolean asString) {
		return get("get", key, hashCode).value;
	}

	@Override
	public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes, boolean asString) {
		return getMulti(keys, hashCodes);
	}

	@Override
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	@Override
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
}
