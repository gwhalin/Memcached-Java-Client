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
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
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

import com.whalin.MemCached.ErrorHandler;
import com.whalin.MemCached.LineInputStream;
import com.whalin.MemCached.MemCachedClient;

/**
 * This client implements the text protocol of memcached in a very high
 * performance way.<br>
 * <br>
 * Please use the wrapper class {@link MemCachedClient} for accessing the
 * memcached server.
 *
 * @author Xingen Wang
 * @since 2.5.0
 * @see BinaryClient
 */
public class AscIIClient extends MemCachedClient {
	private TransBytecode keyTransCoder = new KeyTransCoder();

	private TransCoder transCoder = new ObjectTransCoder();

	// pool instance
	private SchoonerSockIOPool pool;

	// which pool to use
	private String poolName;

	private boolean primitiveAsString;
	@SuppressWarnings("unused")
	private boolean compressEnable;
	@SuppressWarnings("unused")
	private long compressThreshold;
	private String defaultEncoding;

	@Override
	public boolean isUseBinaryProtocol() {
		return false;
	}

	/**
	 * Creates a new instance of MemCachedClient.
	 */
	public AscIIClient() {
		this(null);
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
	public AscIIClient(String poolName) {
		super((MemCachedClient) null);
		this.poolName = poolName;
		init();
	}

	public AscIIClient(String poolName, ClassLoader cl, ErrorHandler eh) {
		super((MemCachedClient) null);
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
		this.primitiveAsString = false;
		this.compressEnable = false;
		this.compressThreshold = COMPRESS_THRESH;
		this.defaultEncoding = "UTF-8";
		this.poolName = (this.poolName == null) ? "default" : this.poolName;

		// get a pool instance to work with for the life of this instance
		this.pool = SchoonerSockIOPool.getInstance(poolName);
	}

	@Override
	public boolean keyExists(Serializable key) {
		return this.get(key, null) != null;
	}

	@Override
	public boolean delete(Serializable key) {
		return delete(key, null, null);
	}

	@Override
	public boolean delete(Serializable key, Date expiry) {
		return delete(key, null, expiry);
	}

	@Override
	public boolean delete(Serializable origin, Integer hashCode, Date expiry) {
		String key = null;

		if (origin == null) {
			log.error("null value for key passed to delete()");
			return false;
		}

		key = keyTransCoder.encode(origin);

		// get SockIO obj from hash or from key
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		// return false if unable to get SockIO obj
		if (sock == null) {
			if (errorHandler != null) {
				errorHandler.handleErrorOnDelete(this, new IOException(
						"no socket to server available"), key);
			}
			return false;
		}

		// build command
		StringBuilder command = new StringBuilder("delete ").append(key);
		if (expiry != null) {
			command.append(" " + expiry.getTime() / 1000);
		}

		command.append("\r\n");

		try {
			sock.write(command.toString().getBytes());

			// if we get appropriate response back, then we return true
			// get result code
			SockInputStream sis = new SockInputStream(sock, Integer.MAX_VALUE);
			String line = sis.getLine();
			sis.close();
			if (DELETED.equals(line)) { // successful
				log.debug(new StringBuffer().append("++++ deletion of key: ")
						.append(key).append(" from cache was a success")
						.toString());
				return true;
			} else if (NOTFOUND.equals(line)) { // key not found
				log.debug(new StringBuffer().append("++++ deletion of key: ")
						.append(key)
						.append(" from cache failed as the key was not found")
						.toString());
			} else { // other error information
				if (log.isErrorEnabled()) {
					log.error(new StringBuffer()
							.append("++++ error deleting key: ").append(key)
							.toString());
					log.error(new StringBuffer()
							.append("++++ server response: ").append(line)
							.toString());
				}
			}
		} catch (IOException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null) {
				errorHandler.handleErrorOnDelete(this, e, key);
			}

			// exception thrown
			if (log.isErrorEnabled()) {
				log.error("++++ exception thrown while writing bytes to server on delete");
				log.error(e.getMessage(), e);
			}

			try {
				sock.sockets.invalidateObject(sock);
			} catch (Exception e1) {
				log.error("++++ failed to close socket : " + sock.toString());
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

	@Override
	public boolean set(Serializable key, Serializable value) {
		return set("set", key, value, null, null, 0L, primitiveAsString);
	}

	@Override
	public boolean set(Serializable key, Serializable value, Integer hashCode) {
		return set("set", key, value, null, hashCode, 0L, primitiveAsString);
	}

	@Override
	public boolean set(Serializable key, Serializable value, Date expiry) {
		return set("set", key, value, expiry, null, 0L, primitiveAsString);
	}

	@Override
	public boolean set(Serializable key, Serializable value, Date expiry,
			Integer hashCode) {
		return set("set", key, value, expiry, hashCode, 0L, primitiveAsString);
	}

	@Override
	public boolean set(Serializable key, Serializable value, Date expiry,
			Integer hashCode, boolean asString) {
		return set("set", key, value, expiry, hashCode, 0L, asString);
	}

	@Override
	public boolean add(Serializable key, Serializable value) {
		return set("add", key, value, null, null, 0L, primitiveAsString);
	}

	@Override
	public boolean add(Serializable key, Serializable value, Integer hashCode) {
		return set("add", key, value, null, hashCode, 0L, primitiveAsString);
	}

	@Override
	public boolean add(Serializable key, Serializable value, Date expiry) {
		return set("add", key, value, expiry, null, 0L, primitiveAsString);
	}

	@Override
	public boolean add(Serializable key, Serializable value, Date expiry,
			Integer hashCode) {
		return set("add", key, value, expiry, hashCode, 0L, primitiveAsString);
	}

	@Override
	public boolean append(Serializable key, Serializable value, Integer hashCode) {
		return set("append", key, value, null, hashCode, 0L, primitiveAsString);
	}

	@Override
	public boolean append(Serializable key, Serializable value) {
		return set("append", key, value, null, null, 0L, primitiveAsString);
	}

	@Override
	public boolean cas(Serializable key, Serializable value, Integer hashCode,
			long casUnique) {
		return set("cas", key, value, null, hashCode, casUnique,
				primitiveAsString);
	}

	@Override
	public boolean cas(Serializable key, Serializable value, Date expiry,
			long casUnique) {
		return set("cas", key, value, expiry, null, casUnique,
				primitiveAsString);
	}

	@Override
	public boolean cas(Serializable key, Serializable value, Date expiry,
			Integer hashCode, long casUnique) {
		return set("cas", key, value, expiry, hashCode, casUnique,
				primitiveAsString);
	}

	@Override
	public boolean cas(Serializable key, Serializable value, long casUnique) {
		return set("cas", key, value, null, null, casUnique, primitiveAsString);
	}

	@Override
	public boolean prepend(Serializable key, Serializable value,
			Integer hashCode) {
		return set("prepend", key, value, null, hashCode, 0L, primitiveAsString);
	}

	@Override
	public boolean prepend(Serializable key, Serializable value) {
		return set("prepend", key, value, null, null, 0L, primitiveAsString);
	}

	@Override
	public boolean replace(Serializable key, Serializable value) {
		return set("replace", key, value, null, null, 0L, primitiveAsString);
	}

	@Override
	public boolean replace(Serializable key, Serializable value,
			Integer hashCode) {
		return set("replace", key, value, null, hashCode, 0L, primitiveAsString);
	}

	@Override
	public boolean replace(Serializable key, Serializable value, Date expiry) {
		return set("replace", key, value, expiry, null, 0L, primitiveAsString);
	}

	@Override
	public boolean replace(Serializable key, Serializable value, Date expiry,
			Integer hashCode) {
		return set("replace", key, value, expiry, hashCode, 0L,
				primitiveAsString);
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
	 * @param asString
	 *            if true, then store all primitives as their string value
	 * @return true/false indicating success
	 */
	private boolean set(String cmdname, Serializable originKey,
			Serializable value, Date expiry, Integer hashCode, Long casUnique,
			boolean asString) {

		if (cmdname == null || originKey == null) {
			log.error("key is null or cmd is null/empty for set()");
			return false;
		}
		String key = keyTransCoder.encode(originKey);

		if (value == null) {
			log.error("trying to store a null value to cache");
			return false;
		}

		// get SockIO obj
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		if (sock == null) {
			if (errorHandler != null) {
				errorHandler.handleErrorOnSet(this, new IOException(
						"no socket to server available"), key);
			}
			return false;
		}

		if (expiry == null) {
			expiry = new Date(0);
		}

		// store flags
		int flags = asString ? MemCachedClient.MARKER_STRING : NativeHandler
				.getMarkerFlag(value);
		// construct the command
		String cmd = new StringBuffer().append(cmdname).append(" ").append(key)
				.append(" ").append(flags).append(" ")
				.append(expiry.getTime() / 1000).append(" ").toString();

		try {
			sock.writeBuf.clear();
			sock.writeBuf.put(cmd.getBytes());
			int offset = sock.writeBuf.position();
			// write blank bytes size.
			sock.writeBuf.put(BLAND_DATA_SIZE);
			if (casUnique != 0) {
				sock.writeBuf.put((" " + casUnique.toString()).getBytes());
			}
			sock.writeBuf.put(B_RETURN);
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
			} else {
				/*
				 * Using default object transcoder to serialize the
				 * non-primitive values.
				 */
				valLen = transCoder.encode(output, value);
			}
			sock.writeBuf.put(B_RETURN);
			// write serialized object
			byte[] objectSize = new Integer(valLen).toString().getBytes();
			int oldPosition = sock.writeBuf.position();
			sock.writeBuf.position(offset);
			// put real object bytes size
			sock.writeBuf.put(objectSize);
			// return to correct position.
			sock.writeBuf.position(oldPosition);

			// write the buffer to server
			// now write the data to the cache server
			sock.flush();
			// get result code
			SockInputStream sis = new SockInputStream(sock, Integer.MAX_VALUE);
			String line = sis.getLine();
			sis.close();
			if (STORED.equals(line)) {
				/*
				 * Successfully set here.
				 */
				return true;
			}
		} catch (Exception e) {
			// if we have an errorHandler, use its hook
			if (errorHandler != null) {
				errorHandler.handleErrorOnSet(this, e, key);
			}

			// exception thrown
			if (log.isErrorEnabled()) {
				log.error("++++ exception thrown while writing bytes to server on set");
				log.error(e.getMessage(), e);
			}

			try {
				sock.sockets.invalidateObject(sock);
			} catch (Exception e1) {
				log.error("++++ failed to close socket : " + sock.toString());
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

	@Override
	public long addOrIncr(Serializable key) {
		return addOrIncr(key, 0, null);
	}

	@Override
	public long addOrIncr(Serializable key, long inc) {
		return addOrIncr(key, inc, null);
	}

	@Override
	public long addOrIncr(Serializable key, long inc, Integer hashCode) {
		boolean ret = add(key, "" + inc, hashCode);

		if (ret) {
			return inc;
		} else {
			return incrdecr("incr", key, inc, hashCode);
		}
	}

	@Override
	public long addOrDecr(Serializable key) {
		return addOrDecr(key, 0, null);
	}

	@Override
	public long addOrDecr(Serializable key, long inc) {
		return addOrDecr(key, inc, null);
	}

	@Override
	public long addOrDecr(Serializable key, long inc, Integer hashCode) {
		boolean ret = add(key, "" + inc, hashCode);
		if (ret) {
			return inc;
		} else {
			return incrdecr("decr", key, inc, hashCode);
		}
	}

	@Override
	public long incr(Serializable key) {
		return incrdecr("incr", key, 1, null);
	}

	@Override
	public long incr(Serializable key, long inc) {
		return incrdecr("incr", key, inc, null);
	}

	@Override
	public long incr(Serializable key, long inc, Integer hashCode) {
		return incrdecr("incr", key, inc, hashCode);
	}

	@Override
	public long decr(Serializable key) {
		return incrdecr("decr", key, 1, null);
	}

	@Override
	public long decr(Serializable key, long inc) {
		return incrdecr("decr", key, inc, null);
	}

	@Override
	public long decr(Serializable key, long inc, Integer hashCode) {
		return incrdecr("decr", key, inc, hashCode);
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
	private long incrdecr(String cmdname, Serializable originKey, long inc,
			Integer hashCode) {

		if (originKey == null) {
			log.error("null key for incrdecr()");
			return -1;
		}

		String key = keyTransCoder.encode(originKey);

		// get SockIO obj for given cache key
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		if (sock == null) {
			if (errorHandler != null) {
				errorHandler.handleErrorOnSet(this, new IOException(
						"no socket to server available"), key);
			}
			return -1;
		}

		try {
			String cmd = new StringBuffer().append(cmdname).append(" ")
					.append(key).append(" ").append(inc).append("\r\n")
					.toString();
			sock.write(cmd.getBytes());
			// get result code
			SockInputStream sis = new SockInputStream(sock, Integer.MAX_VALUE);
			String line = sis.getLine().split("\r\n")[0];
			sis.close();
			if (line.matches("\\d+")) {
				// Sucessfully increase.
				// return sock to pool and return result
				return Long.parseLong(line);
			} else if (NOTFOUND.equals(line + "\r\n")) {
				log.info(new StringBuffer()
						.append("++++ key not found to incr/decr for key: ")
						.append(key).toString());
			} else {
				if (log.isErrorEnabled()) {
					log.error(new StringBuffer()
							.append("++++ error incr/decr key: ").append(key)
							.toString());
					log.error(new StringBuffer()
							.append("++++ server response: ").append(line)
							.toString());
				}
			}
		} catch (Exception e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null) {
				errorHandler.handleErrorOnGet(this, e, key);
			}

			// exception thrown
			if (log.isErrorEnabled()) {
				log.error("++++ exception thrown while writing bytes to server on incr/decr");
				log.error(e.getMessage(), e);
			}

			try {
				sock.sockets.invalidateObject(sock);
			} catch (Exception e1) {
				log.error("++++ failed to close socket : " + sock.toString());
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

	@Override
	public Object get(Serializable key) {
		return get(key, null);
	}

	@Override
	public Object get(Serializable key, Integer hashCode) {
		return get("get", key, hashCode, false);
	}

	@Override
	public MemcachedItem gets(Serializable key) {
		return gets(key, null);
	}

	@Override
	public MemcachedItem gets(Serializable key, Integer hashCode) {
		return gets("gets", key, hashCode, false);
	}

	/**
	 * Retrieve a key from the server, using a specific hash.
	 *
	 * If the data was compressed or serialized when compressed, it will
	 * automatically<br/>
	 * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 * <br/>
	 * Non-serialized data will be returned as a string, so explicit conversion
	 * to<br/>
	 * numeric types will be necessary, if desired<br/>
	 *
	 * @param key
	 *            key where data is stored
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @param asString
	 *            if true, then return string val
	 * @return the object that was previously stored, or null if it was not
	 *         previously stored
	 */
	@Override
	public Object get(Serializable key, Integer hashCode, boolean asString) {
		return get("get", key, hashCode, asString);
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
	private Object get(String cmd, Serializable originKey, Integer hashCode,
			boolean asString) {
		if (originKey == null) {
			log.error("key is null for get()");
			return null;
		}

		String key = keyTransCoder.encode(originKey);

		// get SockIO obj using cache key
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		if (sock == null) {
			if (errorHandler != null) {
				errorHandler.handleErrorOnGet(this, new IOException(
						"no socket to server available"), key);
			}
			return null;
		}

		String cmdLine = cmd + " " + key;

		try {
			sock.writeBuf.clear();
			sock.writeBuf.put(cmdLine.getBytes());
			sock.writeBuf.put(B_RETURN);
			// write buffer to server
			sock.flush();

			int dataSize = 0;
			int flag = 0;

			// get result code
			SockInputStream input = new SockInputStream(sock, Integer.MAX_VALUE);
			// Then analysis the return metadata from server
			// including key, flag and data size
			boolean stop = false;
			StringBuffer sb = new StringBuffer();
			int b;
			int index = 0;
			while (!stop) {
				/*
				 * Critical block to parse the response header.
				 */
				b = input.read();
				if (b == ' ' || b == '\r') {
					switch (index) {
					case 0:
						if (END.startsWith(sb.toString())) {
							return null;
						}
					case 1:
						break;
					case 2:
						flag = Integer.parseInt(sb.toString());
						break;
					case 3:
						// get the data size
						dataSize = Integer.parseInt(sb.toString());
						break;
					}
					index++;
					sb = new StringBuffer();
					if (b == '\r') {
						input.read();
						stop = true;
					}
					continue;
				}
				sb.append((char) b);
			}

			Object o = null;
			input.willRead(dataSize);
			// we can only take out serialized objects
			if (dataSize > 0) {
				if (NativeHandler.isHandled(flag)) {
					// decoding object
					byte[] buf = input.getBuffer();
					if ((flag & F_COMPRESSED) == F_COMPRESSED) {
						GZIPInputStream gzi = new GZIPInputStream(
								new ByteArrayInputStream(buf));
						ByteArrayOutputStream bos = new ByteArrayOutputStream(
								buf.length);
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
					} else {
						o = NativeHandler.decode(buf, flag);
					}
				} else if (transCoder != null) {
					// decode object with default transcoder.
					InputStream in = input;
					if ((flag & F_COMPRESSED) == F_COMPRESSED) {
						in = new GZIPInputStream(in);
					}
					if (classLoader == null) {
						o = transCoder.decode(in);
					} else {
						o = ((ObjectTransCoder) transCoder).decode(in,
								classLoader);
					}
				}
			}
			input.willRead(Integer.MAX_VALUE);
			// Skip "\r\n" after each data block for VALUE
			input.getLine();
			// Skip "END\r\n" after get
			input.getLine();
			return o;
		} catch (Exception ce) {
			// if we have an errorHandler, use its hook
			if (errorHandler != null) {
				errorHandler.handleErrorOnGet(this, ce, key);
			}

			// exception thrown
			if (log.isErrorEnabled()) {
				log.error("++++ exception thrown while trying to get object from cache for key: "
						+ key);
				log.error(ce.getMessage(), ce);
			}

			try {
				sock.sockets.invalidateObject(sock);
			} catch (Exception e1) {
				log.error("++++ failed to close socket : " + sock.toString());
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

	public MemcachedItem gets(String cmd, Serializable originKey,
			Integer hashCode, boolean asString) {

		if (originKey == null) {
			log.error("key is null for get()");
			return null;
		}

		String key = keyTransCoder.encode(originKey);

		// get SockIO obj using cache key
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		if (sock == null) {
			if (errorHandler != null) {
				errorHandler.handleErrorOnGet(this, new IOException(
						"no socket to server available"), key);
			}
			return null;
		}

		String cmdLine = cmd + " " + key;

		try {
			sock.writeBuf.clear();
			sock.writeBuf.put(cmdLine.getBytes());
			sock.writeBuf.put(B_RETURN);
			// write buffer to server
			sock.flush();

			int dataSize = 0;
			int flag = 0;
			MemcachedItem item = new MemcachedItem();

			// get result code
			SockInputStream input = new SockInputStream(sock, Integer.MAX_VALUE);
			// Then analysis the return metadata from server
			// including key, flag and data size
			boolean stop = false;
			StringBuffer sb = new StringBuffer();
			int b;
			int index = 0;
			while (!stop) {
				/*
				 * Critical block to parse the response header.
				 */
				b = input.read();
				if (b == ' ' || b == '\r') {
					switch (index) {
					case 0:
						if (END.startsWith(sb.toString())) {
							return null;
						}
					case 1:
						break;
					case 2:
						flag = Integer.parseInt(sb.toString());
						break;
					case 3:
						// get the data size
						dataSize = Integer.parseInt(sb.toString());
						break;
					case 4:
						if (cmd.equals("gets")) {
							item.casUnique = Long.parseLong(sb.toString());
						}
						break;
					}
					index++;
					sb = new StringBuffer();
					if (b == '\r') {
						input.read();
						stop = true;
					}
					continue;
				}
				sb.append((char) b);
			}
			Object o = null;
			input.willRead(dataSize);
			// we can only take out serialized objects
			if (dataSize > 0) {
				if (NativeHandler.isHandled(flag)) {
					byte[] buf = input.getBuffer();
					if ((flag & F_COMPRESSED) == F_COMPRESSED) {
						GZIPInputStream gzi = new GZIPInputStream(
								new ByteArrayInputStream(buf));
						ByteArrayOutputStream bos = new ByteArrayOutputStream(
								buf.length);
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
					} else {
						// decoding object
						o = NativeHandler.decode(buf, flag);
					}
				} else if (transCoder != null) {
					InputStream in = input;
					if ((flag & F_COMPRESSED) == F_COMPRESSED) {
						in = new GZIPInputStream(in);
					}
					// decode object with default transcoder.
					o = transCoder.decode(in);
				}
			}
			item.value = o;
			input.willRead(Integer.MAX_VALUE);
			// Skip "\r\n" after each data block for VALUE
			input.getLine();
			// Skip "END\r\n" after get
			input.getLine();
			return item;

		} catch (Exception ce) {
			// if we have an errorHandler, use its hook
			if (errorHandler != null) {
				errorHandler.handleErrorOnGet(this, ce, key);
			}

			// exception thrown
			if (log.isErrorEnabled()) {
				log.error("++++ exception thrown while trying to get object from cache for key: "
						+ key);
				log.error(ce.getMessage(), ce);
			}

			try {
				sock.sockets.invalidateObject(sock);
			} catch (Exception e1) {
				log.error("++++ failed to close socket : " + sock.toString());
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

	/**
	 * set key transcoder
	 *
	 * @param keyTransCoder
	 */
	@Override
	public void setKeyTransCoder(TransBytecode keyTransCoder) {
		this.keyTransCoder = keyTransCoder;
	}

	/**
	 * set transcoder. TransCoder is used to customize the serialization and
	 * deserialization.
	 *
	 * @param transCoder
	 */
	@Override
	public void setTransCoder(TransCoder transCoder) {
		this.transCoder = transCoder;
	}

	@Override
	public Object[] getMultiArray(Serializable[] keys) {
		return getMultiArray(keys, null);
	}

	@Override
	public Object[] getMultiArray(Serializable[] keys, Integer[] hashCodes) {
		Map<Serializable, Object> data = getMulti(keys, hashCodes);

		if (data == null) {
			return null;
		}

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
	@Override
	public Object[] getMultiArray(Serializable[] keys, Integer[] hashCodes,
			boolean asString) {
		Map<Serializable, Object> data = getMulti(keys, hashCodes, asString);

		if (data == null) {
			return null;
		}

		Object[] res = new Object[keys.length];
		for (int i = 0; i < keys.length; i++) {
			res[i] = data.get(keys[i]);
		}

		return res;
	}

	@Override
	public Map<Serializable, Object> getMulti(Serializable[] keys) {
		return getMulti(keys, null);
	}

	@Override
	public Map<Serializable, Object> getMulti(Serializable[] keys,
			Integer[] hashCodes) {
		return getMulti(keys, hashCodes, false);
	}

	/**
	 * Retrieve multiple keys from the memcache.
	 *
	 * This is recommended over repeated calls to {@link #get(String) get()},
	 * since it<br/>
	 * is more efficient.<br/>
	 *
	 * @param keys
	 *            keys to retrieve
	 * @param hashCodes
	 *            if not null, then the Integer array of hashCodes
	 * @param asString
	 *            if true then retrieve using String val
	 * @return a hashmap with entries for each key is found by the server, keys
	 *         that are not found are not entered into the hashmap, but
	 *         attempting to retrieve them from the hashmap gives you null.
	 */
	@Override
	public Map<Serializable, Object> getMulti(Serializable[] keys,
			Integer[] hashCodes, boolean asString) {
		if (keys == null || keys.length == 0) {
			log.error("missing keys for getMulti()");
			return null;
		}

		Map<String, StringBuilder> cmdMap = new HashMap<String, StringBuilder>();
		String[] cleanKeys = new String[keys.length];
		for (int i = 0; i < keys.length; ++i) {
			Serializable originKey = keys[i];
			if (originKey == null) {
				log.error("null key, so skipping");
				continue;
			}

			Integer hash = null;
			if (hashCodes != null && hashCodes.length > i) {
				hash = hashCodes[i];
			}

			String key = keyTransCoder.encode(originKey);

			cleanKeys[i] = key;

			// get SockIO obj from cache key
			SchoonerSockIO sock = pool.getSock(cleanKeys[i], hash);

			if (sock == null) {
				if (errorHandler != null) {
					errorHandler.handleErrorOnGet(this, new IOException(
							"no socket to server available"), key);
				}
				continue;
			}

			// store in map and list if not already
			if (!cmdMap.containsKey(sock.getHost())) {
				cmdMap.put(sock.getHost(), new StringBuilder("get"));
			}

			cmdMap.get(sock.getHost()).append(" " + cleanKeys[i]);

			// return to pool
			sock.close();
		}

		log.debug("multi get socket count : " + cmdMap.size());

		// now query memcache
		Map<Serializable, Object> ret = new HashMap<Serializable, Object>(
				keys.length);

		// now use new NIO implementation
		(new NIOLoader(this)).doMulti(asString, cmdMap, keys, ret);

		// fix the return array in case we had to rewrite any of the keys
		for (int i = 0; i < keys.length; ++i) {

			// if key!=cleanKey and result has cleankey
			if (!keys[i].equals(cleanKeys[i]) && ret.containsKey(cleanKeys[i])) {
				ret.put(keys[i], ret.get(cleanKeys[i]));
				ret.remove(cleanKeys[i]);
			}

			// backfill missing keys w/ null value
			// if (!ret.containsKey(keys[i]))
			// ret.put(keys[i], null);
		}

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
	private void loadMulti(LineInputStream input, Map<Serializable, Object> hm,
			boolean asString) throws IOException {

		while (true) {
			String line = input.readLine();

			if (line.startsWith(VALUE)) {
				String[] info = line.split(" ");
				String key = info[1];
				int flag = Integer.parseInt(info[2]);
				int length = Integer.parseInt(info[3]);

				// read obj into buffer
				byte[] buf = new byte[length];
				input.read(buf);
				input.clearEOL();

				// ready object
				Object o = null;
				// we can only take out serialized objects
				if ((flag & F_COMPRESSED) == F_COMPRESSED) {
					GZIPInputStream gzi = new GZIPInputStream(
							new ByteArrayInputStream(buf));
					ByteArrayOutputStream bos = new ByteArrayOutputStream(
							buf.length);
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
					if (primitiveAsString || asString) {
						// pulling out string value
						o = new String(buf, defaultEncoding);
					} else {
						// decoding object
						try {
							o = NativeHandler.decode(buf, flag);
						} catch (Exception e) {
							log.error("++++ Exception thrown while trying to deserialize for key: "
									+ key + " -- " + e.getMessage());
							e.printStackTrace();
						}
					}
				} else if (transCoder != null) {
					o = transCoder.decode(new ByteArrayInputStream(buf));
				}

				// store the object into the cache
				hm.put(key, o);
			} else if (END.startsWith(line)) {
				break;
			}
		}
	}

	@Override
	public boolean flushAll() {
		return flushAll(null);
	}

	@Override
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
				log.error("++++ unable to get connection to : " + servers[i]);
				success = false;
				if (errorHandler != null) {
					errorHandler.handleErrorOnFlush(this, new IOException(
							"no socket to server available"));
				}
				continue;
			}

			// build command
			String command = "flush_all\r\n";

			try {
				sock.write(command.getBytes());
				// if we get appropriate response back, then we return true
				// get result code
				SockInputStream sis = new SockInputStream(sock,
						Integer.MAX_VALUE);
				String line = sis.getLine();
				sis.close();
				success = (OK.equals(line)) ? success && true : false;
			} catch (IOException e) {

				// if we have an errorHandler, use its hook
				if (errorHandler != null) {
					errorHandler.handleErrorOnFlush(this, e);
				}

				// exception thrown
				if (log.isErrorEnabled()) {
					log.error("++++ exception thrown while writing bytes to server on flushAll");
					log.error(e.getMessage(), e);
				}

				try {
					sock.sockets.invalidateObject(sock);
				} catch (Exception e1) {
					log.error("++++ failed to close socket : "
							+ sock.toString());
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

	@Override
	public Map<String, Map<String, String>> stats() {
		return stats(null);
	}

	@Override
	public Map<String, Map<String, String>> stats(String[] servers) {
		return stats(servers, "stats\r\n", STATS);
	}

	@Override
	public Map<String, Map<String, String>> statsItems() {
		return statsItems(null);
	}

	@Override
	public Map<String, Map<String, String>> statsItems(String[] servers) {
		return stats(servers, "stats items\r\n", STATS);
	}

	@Override
	public Map<String, Map<String, String>> statsSlabs() {
		return statsSlabs(null);
	}

	@Override
	public Map<String, Map<String, String>> statsSlabs(String[] servers) {
		return stats(servers, "stats slabs\r\n", STATS);
	}

	@Override
	public Map<String, Map<String, String>> statsCacheDump(int slabNumber,
			int limit) {
		return statsCacheDump(null, slabNumber, limit);
	}

	@Override
	public Map<String, Map<String, String>> statsCacheDump(String[] servers,
			int slabNumber, int limit) {
		return stats(servers,
				String.format("stats cachedump %d %d\r\n", slabNumber, limit),
				ITEM);
	}

	private Map<String, Map<String, String>> stats(String[] servers,
			String command, String lineStart) {
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
				if (errorHandler != null) {
					errorHandler.handleErrorOnStats(this, new IOException(
							"no socket to server available"));
				}
				continue;
			}

			// build command
			try {
				sock.write(command.getBytes());

				// map to hold key value pairs
				Map<String, String> stats = new HashMap<String, String>();
				// get result code
				SockInputStream input = new SockInputStream(sock,
						Integer.MAX_VALUE);
				String line;
				// loop over results
				while ((line = input.getLine()) != null) {

					if (line.startsWith(lineStart)) {
						String[] info = line.split(" ", 3);
						String key = info.length > 1 ? info[1] : null;
						String value = info.length > 2 ? info[2] : null;
						stats.put(key, value);
					} else if (END.startsWith(line)) {
						// finish when we get end from server
						break;
					} else if (line.startsWith(ERROR)
							|| line.startsWith(CLIENT_ERROR)
							|| line.startsWith(SERVER_ERROR)) {
						if (log.isErrorEnabled()) {
							log.error("++++ failed to query stats");
							log.error("++++ server response: " + line);
						}
						break;
					}

					statsMaps.put(servers[i], stats);
				}
				input.close();
			} catch (Exception e) {

				// if we have an errorHandler, use its hook
				if (errorHandler != null) {
					errorHandler.handleErrorOnStats(this, e);
				}

				// exception thrown
				if (log.isErrorEnabled()) {
					log.error("++++ exception thrown while writing bytes to server on stats");
					log.error(e.getMessage(), e);
				}

				try {
					sock.sockets.invalidateObject(sock);
				} catch (Exception e1) {
					log.error("++++ failed to close socket : "
							+ sock.toString());
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
		protected AscIIClient mc;
		protected Connection[] conns;

		public NIOLoader(AscIIClient mc) {
			this.mc = mc;
		}

		private final class Connection {

			public List<ByteBuffer> incoming = new ArrayList<ByteBuffer>();
			public ByteBuffer outgoing;
			public SchoonerSockIO sock;
			public SocketChannel channel;
			private boolean isDone = false;

			public Connection(SchoonerSockIO sock, StringBuilder request)
					throws IOException {
				this.sock = sock;
				outgoing = ByteBuffer.wrap(request.append("\r\n").toString()
						.getBytes());

				channel = sock.getChannel();
				if (channel == null) {
					throw new IOException("dead connection to: "
							+ sock.getHost());
				}

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
					log.warn(
							"++++ memcache: unexpected error closing normally",
							e);
				}

				try {
					sock.sockets.invalidateObject(sock);
				} catch (Exception e1) {
					log.error(
							"++++ failed to close socket : " + sock.toString(),
							e1);
				}
			}

			public boolean isDone() {
				// if we know we're done, just say so
				if (isDone) {
					return true;
				}

				// else find out the hard way
				int strPos = B_END.length - 1;

				int bi = incoming.size() - 1;
				while (bi >= 0 && strPos >= 0) {
					ByteBuffer buf = incoming.get(bi);
					int pos = buf.position() - 1;
					while (pos >= 0 && strPos >= 0) {
						if (buf.get(pos--) != B_END[strPos--]) {
							return false;
						}
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

			@Override
			public String toString() {
				return new StringBuffer().append("Connection to ")
						.append(sock.getHost()).append(" with ")
						.append(incoming.size()).append(" bufs; done is ")
						.append(isDone).toString();
			}
		}

		public void doMulti(boolean asString,
				Map<String, StringBuilder> sockKeys, Serializable[] keys,
				Map<Serializable, Object> ret) {

			long timeRemaining = 0;
			try {
				selector = Selector.open();

				// get the sockets, flip them to non-blocking, and set up data
				// structures
				conns = new Connection[sockKeys.keySet().size()];
				numConns = 0;
				for (Iterator<String> i = sockKeys.keySet().iterator(); i
						.hasNext();) {
					// get SockIO obj from hostname
					String host = i.next();

					SchoonerSockIO sock = pool.getConnection(host);

					if (sock == null) {
						if (errorHandler != null) {
							errorHandler.handleErrorOnGet(this.mc,
									new IOException(
											"no socket to server available"),
									keys);
						}
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
						Iterator<SelectionKey> it = selector.selectedKeys()
								.iterator();
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

					timeRemaining = timeout
							- (System.currentTimeMillis() - startTime);
				}
			} catch (IOException e) {
				return;
			} finally {
				log.debug("Disconnecting; numConns=" + numConns
						+ "  timeRemaining=" + timeRemaining);

				// run through our conns and either return them to the pool
				// or forcibly close them
				try {
					if (selector != null) {
						selector.close();
					}
				} catch (IOException ignoreMe) {
				}

				for (Connection c : conns) {
					if (c != null) {
						c.close();
					}
				}
			}

			// Done! Build the list of results and return them. If we get
			// here by a timeout, then some of the connections are probably
			// not done. But we'll return what we've got...
			for (Connection c : conns) {
				try {
					if (c.incoming.size() > 0 && c.isDone()) {
						loadMulti(new ByteBufArrayInputStream(c.incoming), ret,
								asString);
					}
				} catch (Exception e) {
					// shouldn't happen; we have all the data already
					log.debug("Caught the aforementioned exception on " + c);
				}
			}
		}

		public void doMulti(Map<String, StringBuilder> sockKeys, String[] keys,
				Map<Serializable, Object> ret) {
			doMulti(false, sockKeys, keys, ret);
		}

		private void handleKey(SelectionKey key) throws IOException {
			if (key.isReadable()) {
				readResponse(key);
			} else if (key.isWritable()) {
				writeRequest(key);
			}
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

	@Override
	public boolean sync(Serializable originKey, Integer hashCode) {
		if (originKey == null) {
			log.error("null value for key passed to sync()");
			return false;
		}
		String key = keyTransCoder.encode(originKey);

		// get SockIO obj from hash or from key
		SchoonerSockIO sock = pool.getSock(key, hashCode);

		// return false if unable to get SockIO obj
		if (sock == null) {
			return false;
		}

		// build command
		StringBuilder command = new StringBuilder("sync ").append(key);
		command.append("\r\n");

		try {
			sock.write(command.toString().getBytes());

			// if we get appropriate response back, then we return true
			// get result code
			SockInputStream sis = new SockInputStream(sock, Integer.MAX_VALUE);
			String line = sis.getLine();
			sis.close();
			if (SYNCED.equals(line)) {
				log.info(new StringBuffer().append("++++ sync of key: ")
						.append(key).append(" from cache was a success")
						.toString());

				// return sock to pool and bail here
				return true;
			} else if (NOTFOUND.equals(line)) {
				log.info(new StringBuffer().append("++++ sync of key: ")
						.append(key)
						.append(" from cache failed as the key was not found")
						.toString());
			} else {
				if (log.isErrorEnabled()) {
					log.error(new StringBuffer()
							.append("++++ error sync key: ").append(key)
							.toString());
					log.error(new StringBuffer()
							.append("++++ server response: ").append(line)
							.toString());
				}
			}
		} catch (IOException e) {
			// exception thrown
			if (log.isErrorEnabled()) {
				log.error("++++ exception thrown while writing bytes to server on delete");
				log.error(e.getMessage(), e);
			}

			try {
				sock.sockets.invalidateObject(sock);
			} catch (Exception e1) {
				log.error("++++ failed to close socket : " + sock.toString(),
						e1);
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

	@Override
	public boolean sync(Serializable key) {
		return sync(key, null);
	}

	@Override
	public boolean syncAll() {
		return syncAll(null);
	}

	@Override
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

			// build command
			String command = "sync_all\r\n";

			try {
				sock.write(command.getBytes());
				// if we get appropriate response back, then we return true
				// get result code
				SockInputStream sis = new SockInputStream(sock,
						Integer.MAX_VALUE);
				String line = sis.getLine();
				sis.close();
				success = (SYNCED.equals(line)) ? success && true : false;
			} catch (IOException e) {
				// exception thrown
				if (log.isErrorEnabled()) {
					log.error("++++ exception thrown while writing bytes to server on flushAll");
					log.error(e.getMessage(), e);
				}

				try {
					sock.sockets.invalidateObject(sock);
				} catch (Exception e1) {
					log.error(
							"++++ failed to close socket : " + sock.toString(),
							e1);
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

	@Override
	public void setDefaultEncoding(String defaultEncoding) {
		this.defaultEncoding = defaultEncoding;
	}

	@Override
	public void setPrimitiveAsString(boolean primitiveAsString) {
		this.primitiveAsString = primitiveAsString;
	}
}
