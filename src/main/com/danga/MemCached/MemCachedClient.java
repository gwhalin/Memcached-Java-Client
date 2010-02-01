/**
 * MemCached Java Client Logger
 * Copyright (c) 2007 Greg Whalin
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
 * @author Greg Whalin <greg@meetup.com> 
 * @version 2.0
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

import java.util.Date;
import java.util.Map;

import com.danga.MemCached.ErrorHandler;
import com.schooner.MemCached.AscIIClient;
import com.schooner.MemCached.AscIIUDPClient;
import com.schooner.MemCached.BinaryClient;
import com.schooner.MemCached.MemcachedItem;
import com.schooner.MemCached.TransCoder;

/**
 * Schooner light-weight memcached client. It is of higher performance than
 * other java memcached clients. The main modification is done by Xingen Wang.<br>
 * <br>
 * Instead of the InputStream and OutputStream, we choose the SocketChannel and
 * java.nio package to increase the performance.<br>
 * Here is an example to use schooner memcached client.<br>
 * <br>
 * Firstly, we should initialize the SockIOPool, which is a connection pool.<br>
 * 
 * <h3>An example of initializing using defaults:</h3>
 * 
 * <pre>
 * static {
 * 	String[] serverlist = { &quot;server1.com:port&quot;, &quot;server2.com:port&quot; };
 * 
 * 	SockIOPool pool = SockIOPool.getInstance();
 * 	pool.setServers(serverlist);
 * 	pool.initialize();
 * }
 * </pre>
 * 
 * <br>
 * Then we can create the client object.<br>
 * 
 * <h3>To create cache client object:</h3>
 * 
 * <pre>
 * MemCachedClient mc = new MemCachedClient();
 * </pre>
 * 
 * <h3>To store an object:</h3>
 * 
 * <pre>
 * MemCachedClient mc = new MemCachedClient();
 * String key = &quot;cacheKey1&quot;;
 * Object value = SomeClass.getObject();
 * mc.set(key, value);
 * </pre>
 * 
 * <h3>To delete a cache entry:</h3>
 * 
 * <pre>
 * MemCachedClient mc = new MemCachedClient();
 * String key = &quot;cacheKey1&quot;;
 * mc.delete(key);
 * </pre>
 * 
 * <h3>To retrieve an object from the cache:</h3>
 * 
 * <pre>
 * MemCachedClient mc = new MemCachedClient();
 * String key = &quot;key&quot;;
 * Object value = mc.get(key);
 * </pre>
 * 
 * <h3>To retrieve an multiple objects from the cache</h3>
 * 
 * <pre>
 * MemCachedClient mc = new MemCachedClient();
 * String[] keys = { &quot;key&quot;, &quot;key1&quot;, &quot;key2&quot; };
 * Map&lt;Object&gt; values = mc.getMulti(keys);
 * </pre>
 * 
 * <h3>To flush all items in server(s)</h3>
 * 
 * <pre>
 * MemCachedClient mc = new MemCachedClient();
 * mc.flushAll();
 * </pre>
 * 
 */
public class MemCachedClient {

	MemCachedClient client;

	// optional passed in classloader
	protected ClassLoader classLoader;

	// optional error handler

	protected ErrorHandler errorHandler;

	// logger
	public static Logger log = Logger.getLogger(MemCachedClient.class.getName(), Logger.LEVEL_FATAL);

	// return codes
	/*
	 * start of value line from server
	 */
	public static final String VALUE = "VALUE";

	/*
	 * start of stats line from server
	 */
	public static final String STATS = "STAT";

	/*
	 * start of item line from server
	 */
	public static final String ITEM = "ITEM";

	/*
	 * successful deletion
	 */
	public static final String DELETED = "DELETED\r\n";

	/*
	 * successful synced.
	 */
	public static final String SYNCED = "SYNCED\r\n";

	/*
	 * record not found for delete or incr/decr
	 */
	public static final String NOTFOUND = "NOT_FOUND\r\n";

	/*
	 * successful store of data
	 */
	public static final String STORED = "STORED\r\n";

	/*
	 * success
	 */
	public static final String OK = "OK\r\n";

	/*
	 * end of data from server
	 */
	public static final String END = "END\r\n";

	/*
	 * invalid command name from client
	 */
	public static final String ERROR = "ERROR\r\n";

	/*
	 * client error in input line - invalid protocol
	 */
	public static final String CLIENT_ERROR = "CLIENT_ERROR\r\n";

	// default compression threshold
	public static final int COMPRESS_THRESH = 30720;

	/*
	 * server error
	 */
	public static final String SERVER_ERROR = "SERVER_ERROR\r\n";

	public static final byte[] B_RETURN = "\r\n".getBytes();
	public static final byte[] B_END = "END\r\n".getBytes();
	public static final byte[] B_NOTFOUND = "NOT_FOUND\r\n".getBytes();
	public static final byte[] B_DELETED = "DELETED\r\r".getBytes();
	public static final byte[] B_STORED = "STORED\r\r".getBytes();

	/**
	 * The following static constants are used for the binary protocol.
	 */
	public static final byte MAGIC_REQ = (byte) 0x80;
	public static final byte MAGIC_RESP = (byte) 0x81;
	public static final int F_COMPRESSED = 2;
	public static final int F_SERIALIZED = 8;

	public static final int STAT_NO_ERROR = 0x0000;
	public static final int STAT_KEY_NOT_FOUND = 0x0001;
	public static final int STAT_KEY_EXISTS = 0x0002;
	public static final int STAT_VALUE_TOO_BIG = 0x0003;
	public static final int STAT_INVALID_ARGUMENTS = 0x0004;
	public static final int STAT_ITEM_NOT_STORED = 0x0005;
	public static final int STAT_UNKNOWN_COMMAND = 0x0081;
	public static final int STAT_OUT_OF_MEMORY = 0x0082;

	public static final byte OPCODE_GET = (byte) 0x00;
	public static final byte OPCODE_SET = (byte) 0x01;
	public static final byte OPCODE_ADD = (byte) 0x02;
	public static final byte OPCODE_REPLACE = (byte) 0x03;
	public static final byte OPCODE_DELETE = (byte) 0x04;
	public static final byte OPCODE_INCREMENT = (byte) 0x05;
	public static final byte OPCODE_DECREMENT = (byte) 0x06;
	public static final byte OPCODE_QUIT = (byte) 0x07;
	public static final byte OPCODE_FLUSH = (byte) 0x08;
	public static final byte OPCODE_GETQ = (byte) 0x09;
	public static final byte OPCODE_NOOP = (byte) 0x0A;
	public static final byte OPCODE_VERSION = (byte) 0x0B;
	public static final byte OPCODE_GETK = (byte) 0x0C;
	public static final byte OPCODE_GETKQ = (byte) 0x0D;
	public static final byte OPCODE_APPEND = (byte) 0x0E;
	public static final byte OPCODE_PREPEND = (byte) 0x0F;
	public static final byte OPCODE_STAT = (byte) 0x10;

	/**
	 * This is used for set command only.<br>
	 * The reason for use this that we have to put the size of the object bytes
	 * into the command line. While as a matter of fact, we can't get the real
	 * size before we completed the serialization.<br>
	 * In order to solve this problem, we add this blank area to the size
	 * position in the command line. When we complete the full serialization and
	 * get the bytes size, we will replace this blank are with real bytes size.<br>
	 * This definitely help us decrease the GC of JVM.
	 */
	public final byte[] BLAND_DATA_SIZE = "       ".getBytes();

	/**
	 * values for cache flags
	 */
	public static final int MARKER_BYTE = 1;
	public static final int MARKER_BOOLEAN = 8192;
	public static final int MARKER_INTEGER = 4;
	public static final int MARKER_LONG = 16384;
	public static final int MARKER_CHARACTER = 16;
	public static final int MARKER_STRING = 32;
	public static final int MARKER_STRINGBUFFER = 64;
	public static final int MARKER_FLOAT = 128;
	public static final int MARKER_SHORT = 256;
	public static final int MARKER_DOUBLE = 512;
	public static final int MARKER_DATE = 1024;
	public static final int MARKER_STRINGBUILDER = 2048;
	public static final int MARKER_BYTEARR = 4096;
	public static final int MARKER_OTHERS = 0x00;

	public boolean isUseBinaryProtocol() {
		return client.isUseBinaryProtocol();
	}

	/**
	 * used when you want to create a ASCIIClient/BinaryClient/UDPASCIIClient
	 * instance
	 */
	protected MemCachedClient(String a, String b) {
		client = null;
	}

	/**
	 * Creates a new instance of MemCachedClient.
	 */
	public MemCachedClient() {
		this(null, true, false);
	}

	/**
	 * Creates a new instance of MemCachedClient.<br>
	 * Use binary protocol with TCP.
	 * 
	 * @param binaryProtocal
	 *            whether use binary protocol.
	 */
	public MemCachedClient(boolean binaryProtocal) {
		this(null, true, binaryProtocal);
	}

	/**
	 * Creates a new instance of MemCachedClient accepting a passed in pool
	 * name.
	 * 
	 * @param poolName
	 *            name of SockIOPool
	 */
	public MemCachedClient(String poolName) {
		this(poolName, true, false);
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
	public MemCachedClient(String poolName, boolean binaryProtocal) {
		this(poolName, true, binaryProtocal);
	}

	/**
	 * Create default memcached client.
	 * 
	 * @param isTCP
	 *            true - use tcp protocol <br>
	 *            false - use udp protocol
	 * @param binaryProtocal
	 *            use binary protocol.
	 */
	public MemCachedClient(boolean isTCP, boolean binaryProtocal) {
		this(null, isTCP, binaryProtocal);
	}

	/**
	 * Create memcached client.
	 * 
	 * @param poolName
	 *            name of SockIOPool
	 * @param isTCP
	 *            use tcp protocol
	 * @param binaryProtocal
	 *            use binary protocol.
	 */
	public MemCachedClient(String poolName, boolean isTcp, boolean binaryProtocal) {
		if (binaryProtocal)
			client = new BinaryClient(poolName);
		else
			client = isTcp ? new AscIIClient(poolName) : new AscIIUDPClient(poolName);
	}

	/**
	 * create memcached client.
	 * 
	 * @param poolName
	 *            pool name
	 * @param isTcp
	 *            is tcp protocol?
	 * @param binaryProtocol
	 *            is binary protocol?
	 * @param cl
	 *            classloader.
	 * @param eh
	 *            errorhandler.
	 * @deprecated will be removed in next release. <br>
	 *             Please use customized transcoder
	 *             {@link com.schooner.MemCached.AbstractTransCoder} to achieve
	 *             the same objective.
	 */
	public MemCachedClient(String poolName, boolean isTcp, boolean binaryProtocol, ClassLoader cl, ErrorHandler eh) {
		if (binaryProtocol)
			client = new BinaryClient(poolName, cl, eh);
		else
			client = isTcp ? new AscIIClient(poolName, cl, eh) : new AscIIUDPClient(poolName, cl, eh);
	}

	/**
	 * Creates a new instance of MemCacheClient but acceptes a passed in
	 * ClassLoader.
	 * 
	 * @param classLoader
	 *            ClassLoader object.
	 * @deprecated will be removed in next release. <br>
	 *             Please use customized transcoder
	 *             {@link com.schooner.MemCached.AbstractTransCoder} to achieve
	 *             the same objective.
	 */
	public MemCachedClient(ClassLoader classLoader) {
		this();
		client.setClassLoader(classLoader);
	}

	/**
	 * Creates a new instance of MemCacheClient but acceptes a passed in
	 * ClassLoader and a passed in ErrorHandler.
	 * 
	 * @param classLoader
	 *            ClassLoader object.
	 * @param errorHandler
	 *            ErrorHandler object.
	 * @deprecated will be removed in next release. <br>
	 *             Please use customized transcoder
	 *             {@link com.schooner.MemCached.AbstractTransCoder} to achieve
	 *             the same objective.
	 */
	public MemCachedClient(ClassLoader classLoader, ErrorHandler errorHandler) {
		this(null, true, false, classLoader, errorHandler);
	}

	/**
	 * Creates a new instance of MemCacheClient but acceptes a passed in
	 * ClassLoader, ErrorHandler, and SockIOPool name.
	 * 
	 * @param classLoader
	 *            ClassLoader object.
	 * @param errorHandler
	 *            ErrorHandler object.
	 * @param poolName
	 *            SockIOPool name
	 * @deprecated will be removed in next release. <br>
	 *             Please use customized transcoder
	 *             {@link com.schooner.MemCached.AbstractTransCoder} to achieve
	 *             the same objective.
	 */
	public MemCachedClient(ClassLoader classLoader, ErrorHandler errorHandler, String poolName) {
		this(poolName, true, false, classLoader, errorHandler);
	}

	/**
	 * Sets an optional ClassLoader to be used for serialization.
	 * 
	 * @param classLoader
	 * @deprecated will be removed in next release. <br>
	 *             Please use customized transcoder
	 *             {@link com.schooner.MemCached.AbstractTransCoder} to achieve
	 *             the same objective.
	 */
	public void setClassLoader(ClassLoader classLoader) {
		client.setClassLoader(classLoader);
	}

	/**
	 * Sets an optional ErrorHandler.
	 * 
	 * @param errorHandler
	 * @deprecated will be removed in next release. The purpose of adding this
	 *             support was to make it compatible with previous releases.
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		client.setErrorHandler(errorHandler);
	}

	/**
	 * Enable storing compressed data, provided it meets the threshold
	 * requirements.
	 * 
	 * If enabled, data will be stored in compressed form if it is<br/>
	 * longer than the threshold length set with setCompressThreshold(int)<br/>
	 *<br/>
	 * The default is that compression is enabled.<br/>
	 *<br/>
	 * Even if compression is disabled, compressed data will be automatically<br/>
	 * decompressed.
	 * 
	 * @param compressEnable
	 *            <CODE>true</CODE> to enable compression, <CODE>false</CODE> to
	 *            disable compression
	 * @deprecated will be removed in next release.
	 */
	public void setCompressEnable(boolean compressEnable) {
		client.setCompressEnable(compressEnable);
	}

	/**
	 * Sets the required length for data to be considered for compression.
	 * 
	 * If the length of the data to be stored is not equal or larger than this
	 * value, it will not be compressed.
	 * 
	 * This defaults to 15 KB.
	 * 
	 * @param compressThreshold
	 *            required length of data to consider compression
	 * @deprecated will be removed in next release.
	 */
	public void setCompressThreshold(long compressThreshold) {
		client.setCompressThreshold(compressThreshold);
	}

	/**
	 * Sets default String encoding when storing primitives as Strings. Default
	 * is UTF-8.
	 * 
	 * @param defaultEncoding
	 */
	public void setDefaultEncoding(String defaultEncoding) {
		client.setDefaultEncoding(defaultEncoding);
	}

	/**
	 * Enables storing primitive types as their String values.
	 * 
	 * @param primitiveAsString
	 *            if true, then store all primitives as their string value.
	 */
	public void setPrimitiveAsString(boolean primitiveAsString) {
		client.setPrimitiveAsString(primitiveAsString);
	}

	/**
	 * Enables/disables sanitizing keys by URLEncoding.
	 * 
	 * @param sanitizeKeys
	 *            if true, then URLEncode all keys
	 */
	public void setSanitizeKeys(boolean sanitizeKeys) {
		client.setSanitizeKeys(sanitizeKeys);
	}

	/**
	 * Checks to see if key exists in cache.
	 * 
	 * @param key
	 *            the key to look for
	 * @return true if key found in cache, false if not (or if cache is down)
	 */
	public boolean keyExists(String key) {
		return client.keyExists(key);
	}

	/**
	 * Deletes an object from cache given cache key.
	 * 
	 * @param key
	 *            the key to be removed
	 * @return <code>true</code>, if the data was deleted successfully
	 */
	public boolean delete(String key) {
		return client.delete(key);
	}

	/**
	 * Deletes an object from cache given cache key and expiration date.
	 * 
	 * @param key
	 *            the key to be removed
	 * @param expiry
	 *            when to expire the record.
	 * @return <code>true</code>, if the data was deleted successfully
	 */
	public boolean delete(String key, Date expiry) {
		return client.delete(key, expiry);
	}

	/**
	 * Deletes an object from cache given cache key, a delete time, and an
	 * optional hashcode.
	 * 
	 * The item is immediately made non retrievable.<br/>
	 * Keep in mind {@link #add(String, Object) add} and
	 * {@link #replace(String, Object) replace}<br/>
	 * will fail when used with the same key will fail, until the server reaches
	 * the<br/>
	 * specified time. However, {@link #set(String, Object) set} will succeed,<br/>
	 * and the new value will not be deleted.
	 * 
	 * @param key
	 *            the key to be removed
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @param expiry
	 *            when to expire the record.
	 * @return <code>true</code>, if the data was deleted successfully
	 */
	public boolean delete(String key, Integer hashCode, Date expiry) {
		return client.delete(key, hashCode, expiry);
	}

	/**
	 * Stores data on the server; only the key and the value are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean set(String key, Object value) {
		return client.set(key, value);
	}

	/**
	 * Stores data on the server; only the key and the value are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean set(String key, Object value, Integer hashCode) {
		return client.set(key, value, hashCode);
	}

	/**
	 * Stores data on the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @return true, if the data was successfully stored
	 */
	public boolean set(String key, Object value, Date expiry) {
		return client.set(key, value, expiry);
	}

	/**
	 * Stores data on the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean set(String key, Object value, Date expiry, Integer hashCode) {
		return client.set(key, value, expiry, hashCode);
	}

	/**
	 * Adds data to the server; only the key and the value are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean add(String key, Object value) {
		return client.add(key, value);
	}

	/**
	 * Adds data to the server; the key, value, and an optional hashcode are
	 * passed in.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean add(String key, Object value, Integer hashCode) {
		return client.add(key, value, hashCode);
	}

	/**
	 * Adds data to the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @return true, if the data was successfully stored
	 */
	public boolean add(String key, Object value, Date expiry) {
		return client.add(key, value, expiry);
	}

	/**
	 * Adds data to the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean add(String key, Object value, Date expiry, Integer hashCode) {
		return client.add(key, value, expiry, hashCode);
	}

	/**
	 * Updates data on the server; only the key and the value are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean replace(String key, Object value) {
		return client.replace(key, value);
	}

	/**
	 * Updates data on the server; only the key and the value and an optional
	 * hash are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean replace(String key, Object value, Integer hashCode) {
		return client.replace(key, value, hashCode);
	}

	/**
	 * Updates data on the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @return true, if the data was successfully stored
	 */
	public boolean replace(String key, Object value, Date expiry) {
		return client.replace(key, value, expiry);
	}

	/**
	 * Updates data on the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean replace(String key, Object value, Date expiry, Integer hashCode) {
		return client.replace(key, value, expiry, hashCode);
	}

	/**
	 * Store a counter to memcached given a key
	 * 
	 * @param key
	 *            cache key
	 * @param counter
	 *            number to store
	 * @return true/false indicating success
	 */
	public boolean storeCounter(String key, long counter) {
		return storeCounter(key, new Long(counter));
	}

	/**
	 * Store a counter to memcached given a key
	 * 
	 * @param key
	 *            cache key
	 * @param counter
	 *            number to store
	 * @return true/false indicating success
	 */
	public boolean storeCounter(String key, Long counter) {
		return storeCounter(key, counter, null);
	}

	/**
	 * Store a counter to memcached given a key
	 * 
	 * @param key
	 *            cache key
	 * @param counter
	 *            number to store
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true/false indicating success
	 */
	public boolean storeCounter(String key, Long counter, Integer hashCode) {
		return set(key, counter, hashCode);
	}

	/**
	 * Returns value in counter at given key as long.
	 * 
	 * @param key
	 *            cache ket
	 * @return counter value or -1 if not found
	 */
	public long getCounter(String key) {
		return getCounter(key, null);
	}

	/**
	 * Returns value in counter at given key as long.
	 * 
	 * @param key
	 *            cache ket
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return counter value or -1 if not found
	 */
	public long getCounter(String key, Integer hashCode) {

		if (key == null) {
			log.error("null key for getCounter()");
			return -1;
		}

		long counter = -1;
		try {
			counter = Long.parseLong((String) get(key, hashCode, true));
		} catch (Exception ex) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, ex, key);

			// not found or error getting out
			log.info(String.format("Failed to parse Long value for key: %s", key));
		}

		return counter;
	}

	/**
	 * Thread safe way to initialize and increment a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @return value of incrementer
	 */
	public long addOrIncr(String key) {
		return client.addOrIncr(key);
	}

	/**
	 * Thread safe way to initialize and increment a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            value to set or increment by
	 * @return value of incrementer
	 */
	public long addOrIncr(String key, long inc) {
		return client.addOrIncr(key, inc);
	}

	/**
	 * Thread safe way to initialize and increment a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            value to set or increment by
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return value of incrementer
	 */
	public long addOrIncr(String key, long inc, Integer hashCode) {
		return client.addOrIncr(key, inc, hashCode);
	}

	/**
	 * Thread safe way to initialize and decrement a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @return value of incrementer
	 */
	public long addOrDecr(String key) {
		return client.addOrDecr(key);
	}

	/**
	 * Thread safe way to initialize and decrement a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            value to set or increment by
	 * @return value of incrementer
	 */
	public long addOrDecr(String key, long inc) {
		return client.addOrDecr(key, inc);
	}

	/**
	 * Thread safe way to initialize and decrement a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            value to set or increment by
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return value of incrementer
	 */
	public long addOrDecr(String key, long inc, Integer hashCode) {
		return client.addOrDecr(key, inc, hashCode);
	}

	/**
	 * Increment the value at the specified key by 1, and then return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long incr(String key) {
		return client.incr(key);
	}

	/**
	 * Increment the value at the specified key by passed in val.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            how much to increment by
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long incr(String key, long inc) {
		return client.incr(key, inc);
	}

	/**
	 * Increment the value at the specified key by the specified increment, and
	 * then return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            how much to increment by
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long incr(String key, long inc, Integer hashCode) {
		return client.incr(key, inc, hashCode);
	}

	/**
	 * Decrement the value at the specified key by 1, and then return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long decr(String key) {
		return client.decr(key);
	}

	/**
	 * Decrement the value at the specified key by passed in value, and then
	 * return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            how much to increment by
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long decr(String key, long inc) {
		return client.decr(key, inc);
	}

	/**
	 * Decrement the value at the specified key by the specified increment, and
	 * then return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            how much to increment by
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long decr(String key, long inc, Integer hashCode) {
		return client.decr(key, inc, hashCode);
	}

	/**
	 * Retrieve a key from the server, using a specific hash.
	 * 
	 * If the data was compressed or serialized when compressed, it will
	 * automatically<br/>
	 * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 *<br/>
	 * Non-serialized data will be returned as a string, so explicit conversion
	 * to<br/>
	 * numeric types will be necessary, if desired<br/>
	 * 
	 * @param key
	 *            key where data is stored
	 * @return the object that was previously stored, or null if it was not
	 *         previously stored
	 */
	public Object get(String key) {
		return client.get(key);
	}

	/**
	 * Retrieve a key from the server, using a specific hash.
	 * 
	 * If the data was compressed or serialized when compressed, it will
	 * automatically<br/>
	 * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 *<br/>
	 * Non-serialized data will be returned as a string, so explicit conversion
	 * to<br/>
	 * numeric types will be necessary, if desired<br/>
	 * 
	 * @param key
	 *            key where data is stored
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return the object that was previously stored, or null if it was not
	 *         previously stored
	 */
	public Object get(String key, Integer hashCode) {
		return client.get(key, hashCode);
	}

	public MemcachedItem gets(String key) {
		return client.gets(key);
	}

	public MemcachedItem gets(String key, Integer hashCode) {
		return client.gets(key, hashCode);
	}

	public void setTransCoder(TransCoder transCoder) {
		client.setTransCoder(transCoder);
	}

	/**
	 * Retrieve a key from the server, using a specific hash.
	 * 
	 * If the data was compressed or serialized when compressed, it will
	 * automatically<br/>
	 * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 *<br/>
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
	public Object get(String key, Integer hashCode, boolean asString) {
		return client.get(key, hashCode, asString);
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
	 * @return Object array ordered in same order as key array containing
	 *         results
	 */
	public Object[] getMultiArray(String[] keys) {
		return client.getMultiArray(keys);
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
	 * @return Object array ordered in same order as key array containing
	 *         results
	 */
	public Object[] getMultiArray(String[] keys, Integer[] hashCodes) {
		return client.getMultiArray(keys, hashCodes);
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
		return client.getMultiArray(keys, hashCodes, asString);
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
	 * @return a hashmap with entries for each key is found by the server, keys
	 *         that are not found are not entered into the hashmap, but
	 *         attempting to retrieve them from the hashmap gives you null.
	 */
	public Map<String, Object> getMulti(String[] keys) {
		return getMulti(keys, null);
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
	 * @return a hashmap with entries for each key is found by the server, keys
	 *         that are not found are not entered into the hashmap, but
	 *         attempting to retrieve them from the hashmap gives you null.
	 */
	public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes) {
		return client.getMulti(keys, hashCodes);
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
	public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes, boolean asString) {
		return client.getMulti(keys, hashCodes, asString);
	}

	/**
	 * Invalidates the entire cache.
	 * 
	 * Will return true only if succeeds in clearing all servers.
	 * 
	 * @return success true/false
	 */
	public boolean flushAll() {
		return client.flushAll();
	}

	/**
	 * Invalidates the entire cache.
	 * 
	 * Will return true only if succeeds in clearing all servers. If pass in
	 * null, then will try to flush all servers.
	 * 
	 * @param servers
	 *            optional array of host(s) to flush (host:port)
	 * @return success true/false
	 */
	public boolean flushAll(String[] servers) {
		return client.flushAll(servers);
	}

	/**
	 * Retrieves stats for all servers.
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains stats with stat name as key and value as value.
	 * 
	 * @return Stats map
	 */
	public Map<String, Map<String, String>> stats() {
		return client.stats();
	}

	/**
	 * Retrieves stats for passed in servers (or all servers).
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains stats with stat name as key and value as value.
	 * 
	 * @param servers
	 *            string array of servers to retrieve stats from, or all if this
	 *            is null
	 * @return Stats map
	 */
	public Map<String, Map<String, String>> stats(String[] servers) {
		return client.stats(servers);
	}

	/**
	 * Retrieves stats items for all servers.
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains item stats with itemname:number:field as key and value as value.
	 * 
	 * @return Stats map
	 */
	public Map<String, Map<String, String>> statsItems() {
		return client.statsItems();
	}

	/**
	 * Retrieves stats for passed in servers (or all servers).
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains item stats with itemname:number:field as key and value as value.
	 * 
	 * @param servers
	 *            string array of servers to retrieve stats from, or all if this
	 *            is null
	 * @return Stats map
	 */
	public Map<String, Map<String, String>> statsItems(String[] servers) {
		return client.statsItems(servers);
	}

	/**
	 * Retrieves stats items for all servers.
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains slabs stats with slabnumber:field as key and value as value.
	 * 
	 * @return Stats map
	 */
	public Map<String, Map<String, String>> statsSlabs() {
		return client.statsSlabs();
	}

	/**
	 * Retrieves stats for passed in servers (or all servers).
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains slabs stats with slabnumber:field as key and value as value.
	 * 
	 * @param servers
	 *            string array of servers to retrieve stats from, or all if this
	 *            is null
	 * @return Stats map
	 */
	public Map<String, Map<String, String>> statsSlabs(String[] servers) {
		return client.statsSlabs(servers);
	}

	/**
	 * Retrieves items cachedump for all servers.
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains cachedump stats with the cachekey as key and byte size and unix
	 * timestamp as value.
	 * 
	 * @param slabNumber
	 *            the item number of the cache dump
	 * @return Stats map
	 */
	public Map<String, Map<String, String>> statsCacheDump(int slabNumber, int limit) {
		return client.statsCacheDump(slabNumber, limit);
	}

	/**
	 * Retrieves stats for passed in servers (or all servers).
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains cachedump stats with the cachekey as key and byte size and unix
	 * timestamp as value.
	 * 
	 * @param servers
	 *            string array of servers to retrieve stats from, or all if this
	 *            is null
	 * @param slabNumber
	 *            the item number of the cache dump
	 * @return Stats map
	 */
	public Map<String, Map<String, String>> statsCacheDump(String[] servers, int slabNumber, int limit) {
		return client.statsCacheDump(servers, slabNumber, limit);
	}

	public boolean sync(String key, Integer hashCode) {
		return client.sync(key, hashCode);
	}

	public boolean sync(String key) {
		return client.sync(key);
	}

	public boolean syncAll() {
		return client.syncAll();
	}

	public boolean syncAll(String[] servers) {
		return client.syncAll(servers);
	}

	public boolean append(String key, Object value, Integer hashCode) {
		return client.append(key, value, hashCode);
	}

	public boolean append(String key, Object value) {
		return client.append(key, value);
	}

	public boolean cas(String key, Object value, Integer hashCode, long casUnique) {
		return client.cas(key, value, hashCode, casUnique);
	}

	public boolean cas(String key, Object value, Date expiry, long casUnique) {
		return client.cas(key, value, expiry, casUnique);
	}

	public boolean cas(String key, Object value, Date expiry, Integer hashCode, long casUnique) {
		return client.cas(key, value, expiry, hashCode, casUnique);
	}

	public boolean cas(String key, Object value, long casUnique) {
		return client.cas(key, value, casUnique);
	}

	public boolean prepend(String key, Object value, Integer hashCode) {
		return client.prepend(key, value, hashCode);
	}

	public boolean prepend(String key, Object value) {
		return client.prepend(key, value);
	}
}
