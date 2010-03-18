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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import com.danga.MemCached.ErrorHandler;
import com.danga.MemCached.MemCachedClient;

/**
 * This client implements the binary protocol of memcached in a very high
 * performance way.<br>
 * Please use the wrapper class {@link MemCachedClient2} for accessing the
 * memcached server.
 * 
 * @author Xingen Wang
 * @since 2.5.0
 * @see AscIIClient
 */
public class BinaryClient extends MemCachedClient {

	private TransCoder transCoder = new ObjectTransCoder();

	// pool instance
	private SchoonerSockIOPool pool;

	// which pool to use
	private String poolName;

	// flags
	private boolean sanitizeKeys;
	private boolean primitiveAsString;
	@SuppressWarnings("unused")
	private boolean compressEnable;
	@SuppressWarnings("unused")
	private long compressThreshold;
	private String defaultEncoding = "utf-8";

	public boolean isUseBinaryProtocol() {
		return true;
	}

	/**
	 * Creates a new instance of MemCachedClient.
	 */
	public BinaryClient() {
		this(null);
	}

	/**
	 * Creates a new instance of MemCachedClient accepting a passed in pool
	 * name.
	 * 
	 * @param poolName
	 *            name of SockIOPool
	 */
	public BinaryClient(String poolName) {
		this(poolName, null, null);
	}

	public BinaryClient(String poolName, ClassLoader cl, ErrorHandler eh) {
		super("a", "b");
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
		this.poolName = (this.poolName == null) ? "default" : this.poolName;

		// get a pool instance to work with for the life of this instance
		this.pool = SchoonerSockIOPool.getInstance(poolName);
	}

	public boolean keyExists(String key) {
		return (this.get(key, null) != null);
	}

	public boolean delete(String key) {
		return delete(key, null, null);
	}

	public boolean delete(String key, Date expiry) {
		return delete(key, null, expiry);
	}

	public boolean delete(String key, Integer hashCode, Date expiry) {

		if (key == null) {
			log.error("null value for key passed to delete()");
			return false;
		}

		try {
			key = sanitizeKey(key);
		} catch (UnsupportedEncodingException e) {
			if (errorHandler != null)
				errorHandler.handleErrorOnDelete(this, e, key);

			log.error("failed to sanitize your key!", e);
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
			sock.writeBuf.clear();
			sock.writeBuf.put(MAGIC_REQ);
			sock.writeBuf.put(OPCODE_DELETE);
			byte[] keyBuf = key.getBytes();
			sock.writeBuf.putShort((short) keyBuf.length);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putInt(keyBuf.length);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putLong(0L);
			sock.writeBuf.put(keyBuf);
			sock.flush();
			// if we get appropriate response back, then we return true
			// get result code
			SockInputStream input = new SockInputStream(sock, Integer.MAX_VALUE);
			DataInputStream dis = new DataInputStream(input);
			dis.readInt();
			dis.readShort();
			short status = dis.readShort();

			if (status == STAT_NO_ERROR) {
				if (log.isInfoEnabled())
					log.info("++++ deletion of key: " + key + " from cache was a success");

				// return sock to pool and bail here
				return true;
			} else if (status == STAT_KEY_NOT_FOUND) {
				if (log.isInfoEnabled())
					log.info("++++ deletion of key: " + key + " from cache failed as the key was not found");
			} else {
				log.error("++++ error deleting key: " + key);
				log.error("++++ server response: " + status);
			}
		} catch (IOException e) {
			// if we have an errorHandler, use its hook
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
		} catch (RuntimeException e) {
		} finally {
			if (sock != null) {
				sock.close();
				sock = null;
			}
		}

		return false;
	}

	public boolean set(String key, Object value) {
		return set(OPCODE_SET, key, value, null, null, 0L, primitiveAsString);
	}

	public boolean set(String key, Object value, Integer hashCode) {
		return set(OPCODE_SET, key, value, null, hashCode, 0L, primitiveAsString);
	}

	public boolean set(String key, Object value, Date expiry) {
		return set(OPCODE_SET, key, value, expiry, null, 0L, primitiveAsString);
	}

	public boolean set(String key, Object value, Date expiry, Integer hashCode) {
		return set(OPCODE_SET, key, value, expiry, hashCode, 0L, primitiveAsString);
	}

	public boolean add(String key, Object value) {
		return set(OPCODE_ADD, key, value, null, null, 0L, primitiveAsString);
	}

	public boolean add(String key, Object value, Integer hashCode) {
		return set(OPCODE_ADD, key, value, null, hashCode, 0L, primitiveAsString);
	}

	public boolean add(String key, Object value, Date expiry) {
		return set(OPCODE_ADD, key, value, expiry, null, 0L, primitiveAsString);
	}

	public boolean add(String key, Object value, Date expiry, Integer hashCode) {
		return set(OPCODE_ADD, key, value, expiry, hashCode, 0L, primitiveAsString);
	}

	public boolean append(String key, Object value, Integer hashCode) {
		return apPrepend(OPCODE_APPEND, key, value, hashCode, 0L);
	}

	public boolean append(String key, Object value) {
		return apPrepend(OPCODE_APPEND, key, value, null, 0L);
	}

	public boolean cas(String key, Object value, Integer hashCode, long casUnique) {
		return set(OPCODE_SET, key, value, null, hashCode, casUnique, primitiveAsString);
	}

	public boolean cas(String key, Object value, Date expiry, long casUnique) {
		return set(OPCODE_SET, key, value, expiry, null, casUnique, primitiveAsString);
	}

	public boolean cas(String key, Object value, Date expiry, Integer hashCode, long casUnique) {
		return set(OPCODE_SET, key, value, expiry, hashCode, casUnique, primitiveAsString);
	}

	public boolean cas(String key, Object value, long casUnique) {
		return set(OPCODE_SET, key, value, null, null, casUnique, primitiveAsString);
	}

	public boolean prepend(String key, Object value, Integer hashCode) {
		return apPrepend(OPCODE_PREPEND, key, value, hashCode, 0L);
	}

	public boolean prepend(String key, Object value) {
		return apPrepend(OPCODE_PREPEND, key, value, null, 0L);
	}

	public boolean replace(String key, Object value) {
		return set(OPCODE_REPLACE, key, value, null, null, 0L, primitiveAsString);
	}

	public boolean replace(String key, Object value, Integer hashCode) {
		return set(OPCODE_REPLACE, key, value, null, hashCode, 0L, primitiveAsString);
	}

	public boolean replace(String key, Object value, Date expiry) {
		return set(OPCODE_REPLACE, key, value, expiry, null, 0L, primitiveAsString);
	}

	public boolean replace(String key, Object value, Date expiry, Integer hashCode) {
		return set(OPCODE_REPLACE, key, value, expiry, hashCode, 0L, primitiveAsString);
	}

	/**
	 * Set, Add, Replace data to cache.
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
	private boolean set(byte opcode, String key, Object value, Date expiry, Integer hashCode, long casUnique,
			boolean asString) {

		if (key == null) {
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
			// store flags
			int flags = NativeHandler.getMarkerFlag(value);
			byte[] buf = key.getBytes();
			sock.writeBuf.clear();
			sock.writeBuf.put(MAGIC_REQ);
			sock.writeBuf.put(opcode);
			sock.writeBuf.putShort((short) buf.length);
			sock.writeBuf.put((byte) 0x08);
			sock.writeBuf.put((byte) 0);
			sock.writeBuf.putShort((short) 0);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putLong(casUnique);
			sock.writeBuf.putInt(flags);
			sock.writeBuf.putInt(new Long(expiry.getTime() / 1000).intValue());
			sock.writeBuf.put(buf);
			SockOutputStream output = new SockOutputStream(sock);
			int valLen = 0;
			if (flags != MARKER_OTHERS) {
				byte[] b;
				if (asString) {
					b = value.toString().getBytes(defaultEncoding);
				} else {
					/*
					 * Using NativeHandler to serialize the value
					 */
					b = NativeHandler.encode(value);
				}
				output.write(b);
				valLen = b.length;
				valLen = b.length;

			} else {
				// always serialize for non-primitive types
				valLen = transCoder.encode(output, value);
			}
			// write serialized object
			int bodyLen = 0x08 + buf.length + valLen;
			int oldPosition = sock.writeBuf.position();
			sock.writeBuf.position(8);
			// put real object bytes size
			sock.writeBuf.putInt(bodyLen);
			// return to correct position.
			sock.writeBuf.position(oldPosition);

			// write the buffer to server
			// now write the data to the cache server
			sock.flush();
			// get result code
			DataInputStream dis = new DataInputStream(new SockInputStream(sock, Integer.MAX_VALUE));
			dis.readInt();
			dis.readShort();
			if (STAT_NO_ERROR == dis.readShort()) {
				return true;
			}
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
		} catch (RuntimeException e) {
		} finally {
			if (sock != null) {
				sock.close();
				sock = null;
			}
		}

		return false;
	}

	/**
	 * Append & Prepend data to cache.
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
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true/false indicating success
	 */
	private boolean apPrepend(byte opcode, String key, Object value, Integer hashCode, Long casUnique) {

		if (key == null) {
			log.error("key is null or cmd is null/empty for set()");
			return false;
		}

		try {
			key = sanitizeKey(key);
		} catch (UnsupportedEncodingException e) {
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
			return false;
		}

		try {
			// store flags
			int flags = NativeHandler.getMarkerFlag(value);
			byte[] buf = key.getBytes();
			sock.writeBuf.clear();
			sock.writeBuf.put(MAGIC_REQ);
			sock.writeBuf.put(opcode);
			sock.writeBuf.putShort((short) buf.length);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putLong(0L);
			sock.writeBuf.putLong(casUnique);
			sock.writeBuf.put(buf);
			SockOutputStream output = new SockOutputStream(sock);
			int valLen = 0;
			if (flags != MARKER_OTHERS) {
				byte[] b = NativeHandler.encode(value);
				output.write(b);
				valLen = b.length;
			} else {
				// always serialize for non-primitive types
				valLen = transCoder.encode(output, value);
			}
			// write serialized object
			int bodyLen = buf.length + valLen;
			int oldPosition = sock.writeBuf.position();
			sock.writeBuf.position(8);
			// put real object bytes size
			sock.writeBuf.putInt(bodyLen);
			// return to correct position.
			sock.writeBuf.position(oldPosition);

			// write the buffer to server
			// now write the data to the cache server
			sock.flush();
			// get result code
			DataInputStream dis = new DataInputStream(new SockInputStream(sock, Integer.MAX_VALUE));
			dis.readInt();
			dis.readShort();
			if (STAT_NO_ERROR == dis.readShort()) {
				return true;
			}
		} catch (IOException e) {
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
			return incrdecr(OPCODE_INCREMENT, key, inc, hashCode);
		}
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
			return incrdecr(OPCODE_DECREMENT, key, inc, hashCode);
		}
	}

	public long incr(String key) {
		return incrdecr(OPCODE_INCREMENT, key, 1, null);
	}

	public long incr(String key, long inc) {
		return incrdecr(OPCODE_INCREMENT, key, inc, null);
	}

	public long incr(String key, long inc, Integer hashCode) {
		return incrdecr(OPCODE_INCREMENT, key, inc, hashCode);
	}

	public long decr(String key) {
		return incrdecr(OPCODE_DECREMENT, key, 1, null);
	}

	public long decr(String key, long inc) {
		return incrdecr(OPCODE_DECREMENT, key, inc, null);
	}

	public long decr(String key, long inc, Integer hashCode) {
		return incrdecr(OPCODE_DECREMENT, key, inc, hashCode);
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
	 * @param opcode
	 *            increment/decrement
	 * @param key
	 *            cache key
	 * @param inc
	 *            amount to incr or decr
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return new value or -1 if not exist
	 */
	private long incrdecr(byte opcode, String key, long inc, Integer hashCode) {

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
			sock.writeBuf.clear();
			sock.writeBuf.put(MAGIC_REQ);
			sock.writeBuf.put(opcode);
			byte[] keyBuf = key.getBytes();
			sock.writeBuf.putShort((short) keyBuf.length);// key size
			sock.writeBuf.put((byte) 0X14);
			sock.writeBuf.put((byte) 0);
			sock.writeBuf.putShort((short) 0);
			sock.writeBuf.putInt(keyBuf.length + 20); // body total
			sock.writeBuf.putInt(0);
			sock.writeBuf.putLong(0L);
			sock.writeBuf.putLong(inc);
			sock.writeBuf.putLong(0L);
			sock.writeBuf.putInt(0);
			sock.writeBuf.put(keyBuf);
			sock.flush();
			// get result code
			DataInputStream dis = new DataInputStream(new SockInputStream(sock, Integer.MAX_VALUE));
			dis.readInt();
			dis.readShort();
			short status = dis.readShort();

			if (status == STAT_NO_ERROR) {

				dis.readLong();
				dis.readLong();
				long res = dis.readLong();
				return res;
			} else {
				log.error(new StringBuffer().append("++++ error incr/decr key: ").append(key).toString());
				log.error(new StringBuffer().append("++++ server response: ").append(status).toString());
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

	public Object get(String key) {
		return get(key, null);
	}

	public Object get(String key, Integer hashCode) {
		return get(OPCODE_GET, key, hashCode, false);
	}

	public MemcachedItem gets(String key) {
		return gets(key, null);
	}

	public MemcachedItem gets(String key, Integer hashCode) {
		return gets(OPCODE_GET, key, hashCode, false);
	}

	public void setTransCoder(TransCoder transCoder) {
		this.transCoder = transCoder;
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

	public Map<String, Object> getMulti(String[] keys) {
		return getMulti(keys, null);
	}

	public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes) {

		if (keys == null || keys.length == 0) {
			log.error("missing keys for getMulti()");
			return null;
		}

		Map<String, ArrayList<String>> cmdMap = new HashMap<String, ArrayList<String>>();

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

			// store in map and list if not already
			if (!cmdMap.containsKey(sock.getHost()))
				cmdMap.put(sock.getHost(), new ArrayList<String>());

			cmdMap.get(sock.getHost()).add(key);

			// return to pool
			sock.close();
		}
		// now query memcache
		Map<String, Object> ret = new HashMap<String, Object>(keys.length);

		// now use new NIO implementation
		(new NIOLoader(this)).doMulti(cmdMap, keys, ret);

		return ret;
	}

	public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes, boolean asString) {

		if (keys == null || keys.length == 0) {
			log.error("missing keys for getMulti()");
			return null;
		}

		Map<String, ArrayList<String>> cmdMap = new HashMap<String, ArrayList<String>>();

		for (int i = 0; i < keys.length; ++i) {

			String key = keys[i];
			if (key == null) {
				log.error("null key, so skipping");
				continue;
			}

			Integer hash = null;
			if (hashCodes != null && hashCodes.length > i)
				hash = hashCodes[i];

			String cleanKey = key;
			try {
				cleanKey = sanitizeKey(key);
			} catch (UnsupportedEncodingException e) {

				// if we have an errorHandler, use its hook
				if (errorHandler != null)
					errorHandler.handleErrorOnGet(this, e, key);

				log.error("failed to sanitize your key!", e);
				continue;
			}

			// get SockIO obj from cache key
			SchoonerSockIO sock = pool.getSock(cleanKey, hash);

			if (sock == null) {
				if (errorHandler != null)
					errorHandler.handleErrorOnGet(this, new IOException("no socket to server available"), key);
				continue;
			}

			// store in map and list if not already
			if (!cmdMap.containsKey(sock.getHost()))
				cmdMap.put(sock.getHost(), new ArrayList<String>());

			cmdMap.get(sock.getHost()).add(cleanKey);

			// return to pool
			sock.close();
		}

		log.info("multi get socket count : " + cmdMap.size());

		// now query memcache
		Map<String, Object> ret = new HashMap<String, Object>(keys.length);

		// now use new NIO implementation
		(new NIOLoader(this)).doMulti(asString, cmdMap, keys, ret);

		// fix the return array in case we had to rewrite any of the keys
		for (String key : keys) {

			String cleanKey = key;
			try {
				cleanKey = sanitizeKey(key);
			} catch (UnsupportedEncodingException e) {

				// if we have an errorHandler, use its hook
				if (errorHandler != null)
					errorHandler.handleErrorOnGet(this, e, key);

				log.error("failed to sanitize your key!", e);
				continue;
			}

			if (!key.equals(cleanKey) && ret.containsKey(cleanKey)) {
				ret.put(key, ret.get(cleanKey));
				ret.remove(cleanKey);
			}

			// backfill missing keys w/ null value
			if (!ret.containsKey(key))
				ret.put(key, null);
		}

		if (log.isDebugEnabled())
			log.debug("++++ memcache: got back " + ret.size() + " results");
		return ret;
	}

	/**
	 * This method loads the data from cache into a Map.
	 * 
	 * Pass a SockIO object which is ready to receive data and a HashMap<br/>
	 * to store the results.
	 * 
	 * @param sock
	 *            socket waiting to pass back data
	 * @param hm
	 *            hashmap to store data into
	 * @param asString
	 *            if true, and if we are using NativehHandler, return string val
	 * @throws IOException
	 *             if io exception happens while reading from socket
	 */
	private void loadMulti(DataInputStream input, Map<String, Object> hm) throws IOException {

		while (true) {
			input.readByte();
			byte opcode = input.readByte();
			if (opcode == OPCODE_GETKQ) {
				short keyLen = input.readShort();
				input.readInt();
				int length = input.readInt() - keyLen - 4;
				input.readInt();
				input.readLong();
				int flag = input.readInt();
				byte[] keyBuf = new byte[keyLen];
				input.read(keyBuf);
				String key = new String(keyBuf);

				// read obj into buffer
				byte[] buf = new byte[length];
				input.read(buf);

				// ready object
				Object o = null;
				// we can only take out serialized objects
				if ((flag & F_COMPRESSED) == F_COMPRESSED) {
					GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
					ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);
					int count;
					byte[] tmp = new byte[2048];
					while ((count = gzi.read(tmp)) != -1) {
						bos.write(tmp, 0, count);
					}
					// store uncompressed back to buffer
					buf = bos.toByteArray();
					gzi.close();
				}
				if (flag != MARKER_OTHERS) {
					// decoding object
					try {
						o = NativeHandler.decode(buf, flag);
					} catch (Exception e) {

						// if we have an errorHandler, use its hook
						if (errorHandler != null)
							errorHandler.handleErrorOnGet(this, e, key);

						log.error("++++ Exception thrown while trying to deserialize for key: " + key, e);
						e.printStackTrace();
					}
				} else if (transCoder != null) {
					o = transCoder.decode(new ByteArrayInputStream(buf));
				}
				// store the object into the cache
				hm.put(key, o);
			} else if (opcode == OPCODE_NOOP) {
				break;
			}
		}
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

			// build command
			sock.writeBuf.clear();
			sock.writeBuf.put(MAGIC_REQ);
			sock.writeBuf.put(OPCODE_FLUSH);
			sock.writeBuf.putShort((short) 0);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putLong(0);
			sock.writeBuf.putLong(0);
			// write buffer to server

			try {
				sock.flush();
				// if we get appropriate response back, then we return true
				// get result code
				DataInputStream dis = new DataInputStream(new SockInputStream(sock, Integer.MAX_VALUE));
				dis.readInt();
				dis.readShort();
				success = dis.readShort() == STAT_NO_ERROR ? success && true : false;
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

	public Map<String, Map<String, String>> stats() {
		return stats(null);
	}

	public Map<String, Map<String, String>> stats(String[] servers) {
		return stats(servers, OPCODE_STAT, null);
	}

	public Map<String, Map<String, String>> statsItems() {
		return statsItems(null);
	}

	public Map<String, Map<String, String>> statsItems(String[] servers) {
		return stats(servers, OPCODE_STAT, "items".getBytes());
	}

	public Map<String, Map<String, String>> statsSlabs() {
		return statsSlabs(null);
	}

	public Map<String, Map<String, String>> statsSlabs(String[] servers) {
		return stats(servers, OPCODE_STAT, "slabs".getBytes());
	}

	public Map<String, Map<String, String>> statsCacheDump(int slabNumber, int limit) {
		return statsCacheDump(null, slabNumber, limit);
	}

	public Map<String, Map<String, String>> statsCacheDump(String[] servers, int slabNumber, int limit) {
		return stats(servers, OPCODE_STAT, String.format("cachedump %d %d", slabNumber, limit).getBytes());
	}

	private Map<String, Map<String, String>> stats(String[] servers, byte opcode, byte[] reqKey) {

		// get all servers and iterate over them

		servers = (servers == null) ? pool.getServers() : servers;

		// if no servers, then return early
		if (servers == null || servers.length <= 0) {
			log.error("++++ no servers to check stats");
			return null;
		}

		// array of stats Maps
		Map<String, Map<String, String>> statsMaps = new HashMap<String, Map<String, String>>();

		short statKeyLen;
		int statValLen;
		byte[] key;
		byte[] value;

		for (int i = 0; i < servers.length; i++) {

			SchoonerSockIO sock = pool.getConnection(servers[i]);
			if (sock == null) {
				if (errorHandler != null)
					errorHandler.handleErrorOnStats(this, new IOException("no socket to server available"));
				continue;
			}

			try {
				// map to hold key value pairs
				Map<String, String> stats = new HashMap<String, String>();

				// stat request
				sock.writeBuf.clear();
				sock.writeBuf.put(MAGIC_REQ);
				sock.writeBuf.put(opcode);
				if (reqKey != null) {
					sock.writeBuf.putShort((short) reqKey.length);
				} else {
					sock.writeBuf.putShort((short) 0x0000);
				}
				sock.writeBuf.put((byte) 0x00);
				sock.writeBuf.put((byte) 0x00);
				sock.writeBuf.putShort((short) 0x0000);
				sock.writeBuf.putInt(0);
				sock.writeBuf.putInt(0);
				sock.writeBuf.putLong(0);
				if (reqKey != null) {
					sock.writeBuf.put(reqKey);
				}
				sock.writeBuf.flip();
				sock.getChannel().write(sock.writeBuf);

				// response
				DataInputStream input = new DataInputStream(new SockInputStream(sock, Integer.MAX_VALUE));
				while (true) {
					input.skip(2);
					statKeyLen = input.readShort();
					input.skip(4);
					statValLen = input.readInt() - statKeyLen;
					input.skip(12);
					if (statKeyLen == 0)
						break;
					key = new byte[statKeyLen];
					value = new byte[statValLen];
					input.read(key);
					input.read(value);
					stats.put(new String(key), new String(value));
				}
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

	protected final class NIOLoader {
		protected Selector selector;
		protected int numConns = 0;
		protected BinaryClient mc;
		protected Connection[] conns;

		public NIOLoader(BinaryClient mc) {
			this.mc = mc;
		}

		private final class Connection {

			public List<ByteBuffer> incoming = new ArrayList<ByteBuffer>();
			public ByteBuffer outgoing;
			public SchoonerSockIO sock;
			public SocketChannel channel;
			private boolean isDone = false;
			private final byte[] NOOPFLAG = { MAGIC_RESP, OPCODE_NOOP, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

			public Connection(SchoonerSockIO sock, ArrayList<String> keys) throws IOException {
				this.sock = sock;
				outgoing = ByteBuffer.allocateDirect(10 * 1024);
				outgoing.clear();
				for (String key : keys) {
					byte[] buf = key.getBytes();
					outgoing.put(MAGIC_REQ);
					outgoing.put(OPCODE_GETKQ);
					outgoing.putShort((short) buf.length);
					outgoing.putInt(0);
					outgoing.putInt(buf.length);
					outgoing.putInt(0);
					outgoing.putLong(0L);
					outgoing.put(buf);
				}
				outgoing.put(MAGIC_REQ);
				outgoing.put(OPCODE_NOOP);
				outgoing.putShort((short) 0);
				outgoing.putInt(0);
				outgoing.putLong(0L);
				outgoing.putLong(0L);
				outgoing.flip();
				channel = (SocketChannel) sock.getChannel();
				if (channel == null)
					throw new IOException("dead connection to: " + sock.getHost());

				channel.configureBlocking(false);
				channel.register(selector, SelectionKey.OP_WRITE, this);
			}

			public void close() {
				try {
					if (isDone) {
						channel.configureBlocking(true);
						sock.close();
						return;
					}
				} catch (IOException e) {
					log.warn("++++ memcache: unexpected error closing normally");
				}

				try {
					channel.close();
					sock.trueClose();
				} catch (IOException ignoreMe) {
				}
			}

			public boolean isDone() {
				// if we know we're done, just say so
				if (isDone)
					return true;

				// else find out the hard way
				int strPos = NOOPFLAG.length - 1;

				int bi = incoming.size() - 1;
				while (bi >= 0 && strPos >= 0) {
					ByteBuffer buf = incoming.get(bi);
					int pos = buf.position() - 1;
					while (pos >= 0 && strPos >= 0) {
						if (buf.get(pos--) != NOOPFLAG[strPos--])
							return false;
					}

					bi--;
				}

				isDone = strPos < 0;
				return isDone;
			}

			public ByteBuffer getBuffer() {
				int last = incoming.size() - 1;
				if (last >= 0 && incoming.get(last).hasRemaining()) {
					return incoming.get(last);
				} else {
					ByteBuffer newBuf = ByteBuffer.allocate(8192);
					incoming.add(newBuf);
					return newBuf;
				}
			}

			public String toString() {
				return new StringBuffer().append("Connection to ").append(sock.getHost()).append(" with ").append(
						incoming.size()).append(" bufs; done is ").append(isDone).toString();
			}
		}

		public void doMulti(Map<String, ArrayList<String>> sockKeys, String[] keys, Map<String, Object> ret) {
			doMulti(false, sockKeys, keys, ret);
		}

		public void doMulti(boolean asString, Map<String, ArrayList<String>> sockKeys, String[] keys,
				Map<String, Object> ret) {

			long timeRemaining = 0;
			try {
				selector = Selector.open();

				// get the sockets, flip them to non-blocking, and set up data
				// structures
				conns = new Connection[sockKeys.keySet().size()];
				numConns = 0;
				for (Iterator<String> i = sockKeys.keySet().iterator(); i.hasNext();) {
					// get SockIO obj from hostname
					String host = i.next();

					SchoonerSockIO sock = pool.getConnection(host);

					if (sock == null) {
						return;
					}

					conns[numConns++] = new Connection(sock, sockKeys.get(host));
				}

				// the main select loop; ends when
				// 1) we've received data from all the servers, or
				// 2) we time out
				long startTime = System.currentTimeMillis();

				long timeout = pool.getMaxBusy();
				timeRemaining = timeout;

				while (numConns > 0 && timeRemaining > 0) {
					int n = selector.select(Math.min(timeout, 5000));
					if (n > 0) {
						// we've got some activity; handle it
						Iterator<SelectionKey> it = selector.selectedKeys().iterator();
						while (it.hasNext()) {
							SelectionKey key = it.next();
							it.remove();
							handleKey(key);
						}
					} else {
						// timeout likely... better check
						// TODO: This seems like a problem area that we need to
						// figure out how to handle.
						log.error("selector timed out waiting for activity");
					}

					timeRemaining = timeout - (System.currentTimeMillis() - startTime);
				}
			} catch (IOException e) {
				// errors can happen just about anywhere above, from
				// connection setup to any of the mechanics
				log.error("Caught the exception on " + e);
				return;
			} finally {
				// run through our conns and either return them to the pool
				// or forcibly close them
				try {
					if (selector != null)
						selector.close();
				} catch (IOException ignoreMe) {
				}

				for (Connection c : conns) {
					if (c != null)
						c.close();
				}
			}

			// Done! Build the list of results and return them. If we get
			// here by a timeout, then some of the connections are probably
			// not done. But we'll return what we've got...
			for (Connection c : conns) {
				try {
					if (c.incoming.size() > 0 && c.isDone())
						loadMulti(new DataInputStream(new ByteBufArrayInputStream(c.incoming)), ret);
				} catch (Exception e) {
					// shouldn't happen; we have all the data already
					log.warn("Caught the aforementioned exception on " + c);
				}
			}
		}

		private void handleKey(SelectionKey key) throws IOException {
			if (key.isReadable())
				readResponse(key);
			else if (key.isWritable())
				writeRequest(key);
		}

		public void writeRequest(SelectionKey key) throws IOException {
			ByteBuffer buf = ((Connection) key.attachment()).outgoing;
			SocketChannel sc = (SocketChannel) key.channel();

			if (buf.hasRemaining()) {
				sc.write(buf);
			}

			if (!buf.hasRemaining()) {
				key.interestOps(SelectionKey.OP_READ);
			}
		}

		public void readResponse(SelectionKey key) throws IOException {
			Connection conn = (Connection) key.attachment();
			ByteBuffer buf = conn.getBuffer();
			int count = conn.channel.read(buf);
			if (count > 0) {
				if (conn.isDone()) {
					key.cancel();
					numConns--;
					return;
				}
			}
		}
	}

	public boolean sync(String key, Integer hashCode) {
		return false;
	}

	public boolean sync(String key) {
		return sync(key, null);
	}

	public boolean syncAll() {
		return syncAll(null);
	}

	public boolean syncAll(String[] servers) {
		return false;
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
		return get(OPCODE_GET, key, hashCode, asString);
	}

	private Object get(byte opCode, String key, Integer hashCode, boolean asString) {
		if (key == null) {
			log.error("key is null for get()");
			return null;
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
			return null;
		}

		try {
			byte[] buf = key.getBytes();
			sock.writeBuf.clear();
			sock.writeBuf.put(MAGIC_REQ);
			sock.writeBuf.put(opCode);
			sock.writeBuf.putShort((short) buf.length);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putInt(buf.length);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putLong(0);
			sock.writeBuf.put(buf);
			// write buffer to server
			sock.flush();

			int dataSize = 0;
			int flag = 0;

			// get result code
			SockInputStream input = new SockInputStream(sock, Integer.MAX_VALUE);
			DataInputStream dis = new DataInputStream(input);
			// process the header
			dis.readInt();
			byte extra = dis.readByte();
			dis.readByte();
			if (STAT_NO_ERROR == dis.readShort()) {
				dataSize = dis.readInt() - extra;
				dis.readInt();
				dis.readLong();
			}

			flag = dis.readInt();
			Object o = null;
			input.willRead(dataSize);
			// we can only take out serialized objects
			if (dataSize > 0) {
				if (NativeHandler.isHandled(flag)) {
					// decoding object
					buf = input.getBuffer();
					if ((flag & F_COMPRESSED) == F_COMPRESSED) {
						GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
						ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);
						int count;
						byte[] tmp = new byte[2048];
						while ((count = gzi.read(tmp)) != -1) {
							bos.write(tmp, 0, count);
						}
						// store uncompressed back to buffer
						buf = bos.toByteArray();
						gzi.close();
					}
					if (primitiveAsString || asString) {
						o = new String(buf, defaultEncoding);
					} else
						o = NativeHandler.decode(buf, flag);
				} else if (transCoder != null) {
					// decode object with default transcoder.
					InputStream in = input;
					if ((flag & F_COMPRESSED) == F_COMPRESSED)
						in = new GZIPInputStream(in);
					if (classLoader == null)
						o = transCoder.decode(in);
					else
						o = ((ObjectTransCoder) transCoder).decode(in, classLoader);
				}
			}
			return o;
		} catch (IOException e) {
			if (errorHandler != null)
				errorHandler.handleErrorOnDelete(this, e, key);

			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on get");
			log.error(e.getMessage(), e);
			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error(new StringBuffer().append("++++ failed to close socket : ").append(sock.toString())
						.toString());
			}
			sock = null;
		} catch (RuntimeException e) {
		} finally {
			if (sock != null) {
				sock.close();
				sock = null;
			}
		}
		return null;
	}

	private MemcachedItem gets(byte opCode, String key, Integer hashCode, boolean asString) {
		if (key == null) {
			log.error("key is null for get()");
			return null;
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
			return null;
		}

		try {
			byte[] buf = key.getBytes();
			sock.writeBuf.clear();
			sock.writeBuf.put(MAGIC_REQ);
			sock.writeBuf.put(opCode);
			sock.writeBuf.putShort((short) buf.length);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putInt(buf.length);
			sock.writeBuf.putInt(0);
			sock.writeBuf.putLong(0);
			sock.writeBuf.put(buf);
			// write buffer to server
			sock.flush();

			int dataSize = 0;
			int flag = 0;
			MemcachedItem item = new MemcachedItem();

			// get result code
			SockInputStream input = new SockInputStream(sock, Integer.MAX_VALUE);
			DataInputStream dis = new DataInputStream(input);
			// process the header
			dis.readInt();
			byte extra = dis.readByte();
			dis.readByte();
			if (STAT_NO_ERROR == dis.readShort()) {
				dataSize = dis.readInt() - extra;
				dis.readInt();
				item.casUnique = dis.readLong();
			}

			flag = dis.readInt();
			Object o = null;
			input.willRead(dataSize);
			// we can only take out serialized objects
			if (dataSize > 0) {
				if (NativeHandler.isHandled(flag)) {
					// decoding object
					buf = input.getBuffer();
					if ((flag & F_COMPRESSED) == F_COMPRESSED) {
						GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buf));
						ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.length);
						int count;
						byte[] tmp = new byte[2048];
						while ((count = gzi.read(tmp)) != -1) {
							bos.write(tmp, 0, count);
						}
						// store uncompressed back to buffer
						buf = bos.toByteArray();
						gzi.close();
					}
					if (primitiveAsString || asString) {
						o = new String(buf, defaultEncoding);
					} else
						o = NativeHandler.decode(buf, flag);
				} else if (transCoder != null) {
					// decode object with default transcoder.
					InputStream in = input;
					if ((flag & F_COMPRESSED) == F_COMPRESSED)
						in = new GZIPInputStream(in);
					if (classLoader == null)
						o = transCoder.decode(in);
					else
						o = ((ObjectTransCoder) transCoder).decode(in, classLoader);
				}
			}
			item.value = o;
			return item;
		} catch (IOException e) {
			if (errorHandler != null)
				errorHandler.handleErrorOnDelete(this, e, key);

			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on get");
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
		return null;
	}

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

	@Override
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	@Override
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
}
