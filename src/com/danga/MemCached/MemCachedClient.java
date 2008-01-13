/**
 * MemCached Java client
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
package com.danga.MemCached;

import java.util.*;
import java.util.zip.*;
import java.nio.*;          
import java.net.InetAddress;
import java.nio.charset.*;  
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.io.*;
import java.net.URLEncoder;

import org.apache.log4j.Logger;

/**
 * This is a Java client for the memcached server available from
 *  <a href="http:/www.danga.com/memcached/">http://www.danga.com/memcached/</a>.
 * <br/> 
 * Supports setting, adding, replacing, deleting compressed/uncompressed and<br/>
 * serialized (can be stored as string if object is native class) objects to memcached.<br/>
 * <br/>
 * Now pulls SockIO objects from SockIOPool, which is a connection pool.  The server failover<br/>
 * has also been moved into the SockIOPool class.<br/>
 * This pool needs to be initialized prior to the client working.  See javadocs from SockIOPool.<br/>
 * <br/>
 * Some examples of use follow.<br/>
 * <h3>To create cache client object and set params:</h3>
 * <pre> 
 *	MemCachedClient mc = new MemCachedClient();
 *
 *	// compression is enabled by default	
 *	mc.setCompressEnable(true);
 *
 *	// set compression threshhold to 4 KB (default: 15 KB)	
 *	mc.setCompressThreshold(4096);
 *
 *	// turn on storing primitive types as a string representation
 *	// Should not do this in most cases.	
 *	mc.setPrimitiveAsString(true);
 * </pre>	
 * <h3>To store an object:</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String key   = "cacheKey1";	
 *	Object value = SomeClass.getObject();	
 *	mc.set(key, value);
 * </pre> 
 * <h3>To store an object using a custom server hashCode:</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String key   = "cacheKey1";	
 *	Object value = SomeClass.getObject();	
 *	Integer hash = new Integer(45);	
 *	mc.set(key, value, hash);
 * </pre> 
 * The set method shown above will always set the object in the cache.<br/>
 * The add and replace methods do the same, but with a slight difference.<br/>
 * <ul>
 * 	<li>add -- will store the object only if the server does not have an entry for this key</li>
 * 	<li>replace -- will store the object only if the server already has an entry for this key</li>
 * </ul> 
 * <h3>To delete a cache entry:</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String key   = "cacheKey1";	
 *	mc.delete(key);
 * </pre> 
 * <h3>To delete a cache entry using a custom hash code:</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String key   = "cacheKey1";	
 *	Integer hash = new Integer(45);	
 *	mc.delete(key, hashCode);
 * </pre> 
 * <h3>To store a counter and then increment or decrement that counter:</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String key   = "counterKey";	
 *	mc.storeCounter(key, new Integer(100));
 *	System.out.println("counter after adding      1: " mc.incr(key));	
 *	System.out.println("counter after adding      5: " mc.incr(key, 5));	
 *	System.out.println("counter after subtracting 4: " mc.decr(key, 4));	
 *	System.out.println("counter after subtracting 1: " mc.decr(key));	
 * </pre> 
 * <h3>To store a counter and then increment or decrement that counter with custom hash:</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String key   = "counterKey";	
 *	Integer hash = new Integer(45);	
 *	mc.storeCounter(key, new Integer(100), hash);
 *	System.out.println("counter after adding      1: " mc.incr(key, 1, hash));	
 *	System.out.println("counter after adding      5: " mc.incr(key, 5, hash));	
 *	System.out.println("counter after subtracting 4: " mc.decr(key, 4, hash));	
 *	System.out.println("counter after subtracting 1: " mc.decr(key, 1, hash));	
 * </pre> 
 * <h3>To retrieve an object from the cache:</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String key   = "key";	
 *	Object value = mc.get(key);	
 * </pre> 
 * <h3>To retrieve an object from the cache with custom hash:</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String key   = "key";	
 *	Integer hash = new Integer(45);	
 *	Object value = mc.get(key, hash);	
 * </pre> 
 * <h3>To retrieve an multiple objects from the cache</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String[] keys      = { "key", "key1", "key2" };
 *	Map&lt;Object&gt; values = mc.getMulti(keys);
 * </pre> 
 * <h3>To retrieve an multiple objects from the cache with custom hashing</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	String[] keys      = { "key", "key1", "key2" };
 *	Integer[] hashes   = { new Integer(45), new Integer(32), new Integer(44) };
 *	Map&lt;Object&gt; values = mc.getMulti(keys, hashes);
 * </pre> 
 * <h3>To flush all items in server(s)</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	mc.flushAll();
 * </pre> 
 * <h3>To get stats from server(s)</h3>
 * <pre>
 *	MemCachedClient mc = new MemCachedClient();
 *	Map stats = mc.stats();
 * </pre> 
 *
 * @author greg whalin <greg@meetup.com> 
 * @author Richard 'toast' Russo <russor@msoe.edu>
 * @author Kevin Burton <burton@peerfear.org>
 * @author Robert Watts <robert@wattsit.co.uk>
 * @author Vin Chawla <vin@tivo.com>
 * @version 1.5
 */
public class MemCachedClient {

	// logger
	private static Logger log =
		Logger.getLogger( MemCachedClient.class.getName() );

	// return codes
	private static final String VALUE        = "VALUE";			// start of value line from server
	private static final String STATS        = "STAT";			// start of stats line from server
	private static final String ITEM         = "ITEM";			// start of item line from server
	private static final String DELETED      = "DELETED";		// successful deletion
	private static final String NOTFOUND     = "NOT_FOUND";		// record not found for delete or incr/decr
	private static final String STORED       = "STORED";		// successful store of data
	private static final String NOTSTORED    = "NOT_STORED";	// data not stored
	private static final String OK           = "OK";			// success
	private static final String END          = "END";			// end of data from server

	private static final String ERROR        = "ERROR";			// invalid command name from client
	private static final String CLIENT_ERROR = "CLIENT_ERROR";	// client error in input line - invalid protocol
	private static final String SERVER_ERROR = "SERVER_ERROR";	// server error

	private static final byte[] B_END        = "END\r\n".getBytes();
	private static final byte[] B_NOTFOUND   = "NOT_FOUND\r\n".getBytes();
	private static final byte[] B_DELETED    = "DELETED\r\r".getBytes();
	private static final byte[] B_STORED     = "STORED\r\r".getBytes();

	// default compression threshold
	private static final int COMPRESS_THRESH = 30720;
    
	// values for cache flags 
	public static final int MARKER_BYTE             = 1;
	public static final int MARKER_BOOLEAN          = 8192;
	public static final int MARKER_INTEGER          = 4;
	public static final int MARKER_LONG             = 16384;
	public static final int MARKER_CHARACTER        = 16;
	public static final int MARKER_STRING           = 32;
	public static final int MARKER_STRINGBUFFER     = 64;
	public static final int MARKER_FLOAT            = 128;
	public static final int MARKER_SHORT            = 256;
	public static final int MARKER_DOUBLE           = 512;
	public static final int MARKER_DATE             = 1024;
	public static final int MARKER_STRINGBUILDER    = 2048;
	public static final int MARKER_BYTEARR          = 4096;
	public static final int F_COMPRESSED            = 2;
	public static final int F_SERIALIZED            = 8;
	
	// flags
	private boolean sanitizeKeys;
	private boolean primitiveAsString;
	private boolean compressEnable;
	private long compressThreshold;
	private String defaultEncoding;

	// pool instance
	private SockIOPool pool;

	// which pool to use
	private String poolName;

	// optional passed in classloader
	private ClassLoader classLoader;

	// optional error handler
	private ErrorHandler errorHandler;

	/**
	 * Creates a new instance of MemCachedClient.
	 */
	public MemCachedClient() {
		init();
	}

	/** 
	 * Creates a new instance of MemCachedClient
	 * accepting a passed in pool name.
	 * 
	 * @param poolName name of SockIOPool
	 */
	public MemCachedClient( String poolName ) {
		this.poolName = poolName;
		init();
	}

	/** 
	 * Creates a new instance of MemCacheClient but
	 * acceptes a passed in ClassLoader.
	 * 
	 * @param classLoader ClassLoader object.
	 */
	public MemCachedClient( ClassLoader classLoader ) {
		this.classLoader = classLoader;
		init();
	}

	/** 
	 * Creates a new instance of MemCacheClient but
	 * acceptes a passed in ClassLoader and a passed
	 * in ErrorHandler.
	 * 
	 * @param classLoader ClassLoader object.
	 * @param errorHandler ErrorHandler object.
	 */
	public MemCachedClient( ClassLoader classLoader, ErrorHandler errorHandler ) {
		this.classLoader  = classLoader;
		this.errorHandler = errorHandler;
		init();
	}

	/** 
	 * Creates a new instance of MemCacheClient but
	 * acceptes a passed in ClassLoader, ErrorHandler,
	 * and SockIOPool name.
	 * 
	 * @param classLoader ClassLoader object.
	 * @param errorHandler ErrorHandler object.
	 * @param poolName SockIOPool name
	 */
	public MemCachedClient( ClassLoader classLoader, ErrorHandler errorHandler, String poolName ) {
		this.classLoader  = classLoader;
		this.errorHandler = errorHandler;
		this.poolName     = poolName;
		init();
	}

	/** 
	 * Initializes client object to defaults.
	 *
	 * This enables compression and sets compression threshhold to 15 KB.
	 */
	private void init() {
		this.sanitizeKeys       = true;
		this.primitiveAsString  = false;
		this.compressEnable     = true;
		this.compressThreshold  = COMPRESS_THRESH;
		this.defaultEncoding    = "UTF-8";
		this.poolName           = ( this.poolName == null ) ? "default" : this.poolName;

		// get a pool instance to work with for the life of this instance
		this.pool               = SockIOPool.getInstance( poolName );
	}

	/** 
	 * Sets an optional ClassLoader to be used for
	 * serialization.
	 * 
	 * @param classLoader 
	 */
	public void setClassLoader( ClassLoader classLoader ) {
		this.classLoader = classLoader;
	}

	/** 
	 * Sets an optional ErrorHandler.
	 * 
	 * @param errorHandler 
	 */
	public void setErrorHandler( ErrorHandler errorHandler ) {
		this.errorHandler = errorHandler;
	}

	/** 
	 * Enables/disables sanitizing keys by URLEncoding.
	 * 
	 * @param sanitizeKeys if true, then URLEncode all keys
	 */
	public void setSanitizeKeys( boolean sanitizeKeys ) {
		this.sanitizeKeys = sanitizeKeys;
	}

	/** 
	 * Enables storing primitive types as their String values. 
	 * 
	 * @param primitiveAsString if true, then store all primitives as their string value.
	 */
	public void setPrimitiveAsString( boolean primitiveAsString ) {
		this.primitiveAsString = primitiveAsString;
	}

	/** 
	 * Sets default String encoding when storing primitives as Strings. 
	 * Default is UTF-8.
	 * 
	 * @param defaultEncoding 
	 */
	public void setDefaultEncoding( String defaultEncoding ) {
		this.defaultEncoding = defaultEncoding;
	}

	/**
	 * Enable storing compressed data, provided it meets the threshold requirements.
	 *
	 * If enabled, data will be stored in compressed form if it is<br/>
	 * longer than the threshold length set with setCompressThreshold(int)<br/>
	 *<br/>
	 * The default is that compression is enabled.<br/>
	 *<br/>
	 * Even if compression is disabled, compressed data will be automatically<br/>
	 * decompressed.
	 *
	 * @param compressEnable <CODE>true</CODE> to enable compression, <CODE>false</CODE> to disable compression
	 */
	public void setCompressEnable( boolean compressEnable ) {
		this.compressEnable = compressEnable;
	}
    
	/**
	 * Sets the required length for data to be considered for compression.
	 *
	 * If the length of the data to be stored is not equal or larger than this value, it will
	 * not be compressed.
	 *
	 * This defaults to 15 KB.
	 *
	 * @param compressThreshold required length of data to consider compression
	 */
	public void setCompressThreshold( long compressThreshold ) {
		this.compressThreshold = compressThreshold;
	}

	/** 
	 * Checks to see if key exists in cache. 
	 * 
	 * @param key the key to look for
	 * @return true if key found in cache, false if not (or if cache is down)
	 */
	public boolean keyExists( String key ) {
		return ( this.get( key, null, true ) != null );
	}

	/**
	 * Deletes an object from cache given cache key.
	 *
	 * @param key the key to be removed
	 * @return <code>true</code>, if the data was deleted successfully
	 */
	public boolean delete( String key ) {
		return delete( key, null, null );
	}

	/** 
	 * Deletes an object from cache given cache key and expiration date. 
	 * 
	 * @param key the key to be removed
	 * @param expiry when to expire the record.
	 * @return <code>true</code>, if the data was deleted successfully
	 */
	public boolean delete( String key, Date expiry ) {
		return delete( key, null, expiry );
	}

	/**
	 * Deletes an object from cache given cache key, a delete time, and an optional hashcode.
	 *
	 *  The item is immediately made non retrievable.<br/>
	 *  Keep in mind {@link #add(String, Object) add} and {@link #replace(String, Object) replace}<br/>
	 *  will fail when used with the same key will fail, until the server reaches the<br/>
	 *  specified time. However, {@link #set(String, Object) set} will succeed,<br/>
	 *  and the new value will not be deleted.
	 *
	 * @param key the key to be removed
	 * @param hashCode if not null, then the int hashcode to use
	 * @param expiry when to expire the record.
	 * @return <code>true</code>, if the data was deleted successfully
	 */
	public boolean delete( String key, Integer hashCode, Date expiry ) {

		if ( key == null ) {
			log.error( "null value for key passed to delete()" );
			return false;
		}

		try {
			key = sanitizeKey( key );
		}
		catch ( UnsupportedEncodingException e ) {

			// if we have an errorHandler, use its hook
			if ( errorHandler != null )
				errorHandler.handleErrorOnDelete( this, e, key );

			log.error( "failed to sanitize your key!", e );
			return false;
		}

		// get SockIO obj from hash or from key
		if ( pool == null )
			pool = SockIOPool.getInstance( poolName );

		SockIOPool.SockIO sock = pool.getSock( key, hashCode );

		// return false if unable to get SockIO obj
		if ( sock == null )
			return false;

		// build command
		StringBuilder command = new StringBuilder( "delete " ).append( key );
		if ( expiry != null )
			command.append( " " + expiry.getTime() / 1000 );

		command.append( "\r\n" );
		
		try {
			sock.write( command.toString().getBytes() );
			sock.flush();
			
			// if we get appropriate response back, then we return true
			String line = sock.readLine();
			if ( DELETED.equals( line ) ) {
				log.info( "++++ deletion of key: " + key + " from cache was a success" );

				// return sock to pool and bail here
				sock.close();
				sock = null;
				return true;
			}
			else if ( NOTFOUND.equals( line ) ) {
				log.info( "++++ deletion of key: " + key + " from cache failed as the key was not found" );
			}
			else {
				log.error( "++++ error deleting key: " + key );
				log.error( "++++ server response: " + line );
			}
		}
		catch ( IOException e ) {

			// if we have an errorHandler, use its hook
			if ( errorHandler != null )
				errorHandler.handleErrorOnDelete( this, e, key );

			// exception thrown
			log.error( "++++ exception thrown while writing bytes to server on delete" );
			log.error( e.getMessage(), e );

			try {
				sock.trueClose();
			}
			catch ( IOException ioe ) {
				log.error( "++++ failed to close socket : " + sock.toString() );
			}

			sock = null;
		}

		if ( sock != null ) {
			sock.close();
			sock = null;
		}

		return false;
	}
    
	/**
	 * Stores data on the server; only the key and the value are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean set( String key, Object value ) {
		return set( "set", key, value, null, null, primitiveAsString );
	}

	/**
	 * Stores data on the server; only the key and the value are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @param hashCode if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean set( String key, Object value, Integer hashCode ) {
		return set( "set", key, value, null, hashCode, primitiveAsString );
	}

	/**
	 * Stores data on the server; the key, value, and an expiration time are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @param expiry when to expire the record
	 * @return true, if the data was successfully stored
	 */
	public boolean set( String key, Object value, Date expiry ) {
		return set( "set", key, value, expiry, null, primitiveAsString );
	}

	/**
	 * Stores data on the server; the key, value, and an expiration time are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @param expiry when to expire the record
	 * @param hashCode if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean set( String key, Object value, Date expiry, Integer hashCode ) {
		return set( "set", key, value, expiry, hashCode, primitiveAsString );
	}

	/**
	 * Adds data to the server; only the key and the value are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean add( String key, Object value ) {
		return set( "add", key, value, null, null, primitiveAsString );
	}

	/**
	 * Adds data to the server; the key, value, and an optional hashcode are passed in.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @param hashCode if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean add( String key, Object value, Integer hashCode ) {
		return set( "add", key, value, null, hashCode, primitiveAsString );
	}

	/**
	 * Adds data to the server; the key, value, and an expiration time are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @param expiry when to expire the record
	 * @return true, if the data was successfully stored
	 */
	public boolean add( String key, Object value, Date expiry ) {
		return set( "add", key, value, expiry, null, primitiveAsString );
	}

	/**
	 * Adds data to the server; the key, value, and an expiration time are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @param expiry when to expire the record
	 * @param hashCode if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean add( String key, Object value, Date expiry, Integer hashCode ) {
		return set( "add", key, value, expiry, hashCode, primitiveAsString );
	}

	/**
	 * Updates data on the server; only the key and the value are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean replace( String key, Object value ) {
		return set( "replace", key, value, null, null, primitiveAsString );
	}

	/**
	 * Updates data on the server; only the key and the value and an optional hash are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @param hashCode if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean replace( String key, Object value, Integer hashCode ) {
		return set( "replace", key, value, null, hashCode, primitiveAsString );
	}

	/**
	 * Updates data on the server; the key, value, and an expiration time are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @param expiry when to expire the record
	 * @return true, if the data was successfully stored
	 */
	public boolean replace( String key, Object value, Date expiry ) {
		return set( "replace", key, value, expiry, null, primitiveAsString );
	}

	/**
	 * Updates data on the server; the key, value, and an expiration time are specified.
	 *
	 * @param key key to store data under
	 * @param value value to store
	 * @param expiry when to expire the record
	 * @param hashCode if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean replace( String key, Object value, Date expiry, Integer hashCode ) {
		return set( "replace", key, value, expiry, hashCode, primitiveAsString );
	}

	/** 
	 * Stores data to cache.
	 *
	 * If data does not already exist for this key on the server, or if the key is being<br/>
	 * deleted, the specified value will not be stored.<br/>
	 * The server will automatically delete the value when the expiration time has been reached.<br/>
	 * <br/>
	 * If compression is enabled, and the data is longer than the compression threshold<br/>
	 * the data will be stored in compressed form.<br/>
	 * <br/>
	 * As of the current release, all objects stored will use java serialization.
	 * 
	 * @param cmdname action to take (set, add, replace)
	 * @param key key to store cache under
	 * @param value object to cache
	 * @param expiry expiration
	 * @param hashCode if not null, then the int hashcode to use
	 * @param asString store this object as a string?
	 * @return true/false indicating success
	 */
	private boolean set( String cmdname, String key, Object value, Date expiry, Integer hashCode, boolean asString ) {

		if ( cmdname == null || cmdname.trim().equals( "" ) || key == null ) {
			log.error( "key is null or cmd is null/empty for set()" );
			return false;
		}

		try {
			key = sanitizeKey( key );
		}
		catch ( UnsupportedEncodingException e ) {

			// if we have an errorHandler, use its hook
			if ( errorHandler != null )
				errorHandler.handleErrorOnSet( this, e, key );

			log.error( "failed to sanitize your key!", e );
			return false;
		}

		if ( value == null ) {
			log.error( "trying to store a null value to cache" );
			return false;
		}

		// get SockIO obj
		if ( pool == null )
			pool = SockIOPool.getInstance( poolName );

		SockIOPool.SockIO sock = pool.getSock( key, hashCode );
		
		if ( sock == null )
			return false;
		
		if ( expiry == null )
			expiry = new Date(0);

		// store flags
		int flags = 0;
		
		// byte array to hold data
		byte[] val;

        if ( NativeHandler.isHandled( value ) ) {
			
			if ( asString ) {
				// useful for sharing data between java and non-java
				// and also for storing ints for the increment method
				try {
					log.info( "++++ storing data as a string for key: " + key + " for class: " + value.getClass().getName() );
					val = value.toString().getBytes( defaultEncoding );
				}
				catch ( UnsupportedEncodingException ue ) {

					// if we have an errorHandler, use its hook
					if ( errorHandler != null )
						errorHandler.handleErrorOnSet( this, ue, key );

					log.error( "invalid encoding type used: " + defaultEncoding, ue );
					sock.close();
					sock = null;
					return false;
				}
			}
			else {
				try {
					log.info( "Storing with native handler..." );
					flags |= NativeHandler.getMarkerFlag( value );
					val    = NativeHandler.encode( value );
				}
				catch ( Exception e ) {

					// if we have an errorHandler, use its hook
					if ( errorHandler != null )
						errorHandler.handleErrorOnSet( this, e, key );

					log.error( "Failed to native handle obj", e );

					sock.close();
					sock = null;
					return false;
				}
			}
		}
		else {
			// always serialize for non-primitive types
			try {
				log.info( "++++ serializing for key: " + key + " for class: " + value.getClass().getName() );
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				(new ObjectOutputStream( bos )).writeObject( value );
				val = bos.toByteArray();
				flags |= F_SERIALIZED;
			}
			catch ( IOException e ) {

				// if we have an errorHandler, use its hook
				if ( errorHandler != null )
					errorHandler.handleErrorOnSet( this, e, key );

				// if we fail to serialize, then
				// we bail
				log.error( "failed to serialize obj", e );
				log.error( value.toString() );

				// return socket to pool and bail
				sock.close();
				sock = null;
				return false;
			}
		}
		
		// now try to compress if we want to
		// and if the length is over the threshold 
		if ( compressEnable && val.length > compressThreshold ) {

			try {
				log.info( "++++ trying to compress data" );
				log.info( "++++ size prior to compression: " + val.length );
				ByteArrayOutputStream bos = new ByteArrayOutputStream( val.length );
				GZIPOutputStream gos = new GZIPOutputStream( bos );
				gos.write( val, 0, val.length );
				gos.finish();
				
				// store it and set compression flag
				val = bos.toByteArray();
				flags |= F_COMPRESSED;

				log.info( "++++ compression succeeded, size after: " + val.length );
			}
			catch ( IOException e ) {

				// if we have an errorHandler, use its hook
				if ( errorHandler != null )
					errorHandler.handleErrorOnSet( this, e, key );

				log.error( "IOException while compressing stream: " + e.getMessage() );
				log.error( "storing data uncompressed" );
			}
		}

		// now write the data to the cache server
		try {
			String cmd = String.format( "%s %s %d %d %d\r\n", cmdname, key, flags, (expiry.getTime() / 1000), val.length );
			sock.write( cmd.getBytes() );
			sock.write( val );
			sock.write( "\r\n".getBytes() );
			sock.flush();

			// get result code
			String line = sock.readLine();
			log.info( "++++ memcache cmd (result code): " + cmd + " (" + line + ")" );

			if ( STORED.equals( line ) ) {
				log.info("++++ data successfully stored for key: " + key );
				sock.close();
				sock = null;
				return true;
			}
			else if ( NOTSTORED.equals( line ) ) {
				log.info( "++++ data not stored in cache for key: " + key );
			}
			else {
				log.error( "++++ error storing data in cache for key: " + key + " -- length: " + val.length );
				log.error( "++++ server response: " + line );
			}
		}
		catch ( IOException e ) {

			// if we have an errorHandler, use its hook
			if ( errorHandler != null )
				errorHandler.handleErrorOnSet( this, e, key );

			// exception thrown
			log.error( "++++ exception thrown while writing bytes to server on set" );
			log.error( e.getMessage(), e );

			try {
				sock.trueClose();
			}
			catch ( IOException ioe ) {
				log.error( "++++ failed to close socket : " + sock.toString() );
			}

			sock = null;
		}

		if ( sock != null ) {
			sock.close();
			sock = null;
		}

		return false;
	}

	/** 
	 * Store a counter to memcached given a key
	 * 
	 * @param key cache key
	 * @param counter number to store
	 * @return true/false indicating success
	 */
	public boolean storeCounter( String key, long counter ) {
		return set( "set", key, new Long( counter ), null, null, true );
	}

	/** 
	 * Store a counter to memcached given a key
	 * 
	 * @param key cache key
	 * @param counter number to store
	 * @return true/false indicating success
	 */
	public boolean storeCounter( String key, Long counter ) {
		return set( "set", key, counter, null, null, true );
	}
    
	/** 
	 * Store a counter to memcached given a key
	 * 
	 * @param key cache key
	 * @param counter number to store
	 * @param hashCode if not null, then the int hashcode to use
	 * @return true/false indicating success
	 */
	public boolean storeCounter( String key, Long counter, Integer hashCode ) {
		return set( "set", key, counter, null, hashCode, true );
	}

	/** 
	 * Returns value in counter at given key as long. 
	 *
	 * @param key cache ket
	 * @return counter value or -1 if not found
	 */
	public long getCounter( String key ) {
		return getCounter( key, null );
	}

	/** 
	 * Returns value in counter at given key as long. 
	 *
	 * @param key cache ket
	 * @param hashCode if not null, then the int hashcode to use
	 * @return counter value or -1 if not found
	 */
	public long getCounter( String key, Integer hashCode ) {

		if ( key == null ) {
			log.error( "null key for getCounter()" );
			return -1;
		}

		long counter = -1;
		try {
			counter = Long.parseLong( (String)get( key, hashCode, true ) );
		}
		catch ( Exception ex ) {

			// if we have an errorHandler, use its hook
			if ( errorHandler != null )
				errorHandler.handleErrorOnGet( this, ex, key );

			// not found or error getting out
			log.info( String.format( "Failed to parse Long value for key: %s", key ) );
		}
		
		return counter;
	}

	/** 
	 * Thread safe way to initialize and increment a counter. 
	 * 
	 * @param key key where the data is stored
	 * @return value of incrementer
	 */
	public long addOrIncr( String key ) {
		return addOrIncr( key, 0, null );
	}

	/** 
	 * Thread safe way to initialize and increment a counter. 
	 * 
	 * @param key key where the data is stored
	 * @param inc value to set or increment by
	 * @return value of incrementer
	 */
	public long addOrIncr( String key, long inc ) {
		return addOrIncr( key, inc, null );
	}

	/** 
	 * Thread safe way to initialize and increment a counter. 
	 * 
	 * @param key key where the data is stored
	 * @param inc value to set or increment by
	 * @param hashCode if not null, then the int hashcode to use
	 * @return value of incrementer
	 */
	public long addOrIncr( String key, long inc, Integer hashCode ) {
		boolean ret = set( "add", key, new Long( inc ), null, hashCode, true );

		if ( ret ) {
			return inc;
		}
		else {
			return incrdecr( "incr", key, inc, hashCode );
		}
	}

	/** 
	 * Thread safe way to initialize and decrement a counter. 
	 * 
	 * @param key key where the data is stored
	 * @return value of incrementer
	 */
	public long addOrDecr( String key ) {
		return addOrDecr( key, 0, null );
	}

	/** 
	 * Thread safe way to initialize and decrement a counter. 
	 * 
	 * @param key key where the data is stored
	 * @param inc value to set or increment by
	 * @return value of incrementer
	 */
	public long addOrDecr( String key, long inc ) {
		return addOrDecr( key, inc, null );
	}

	/** 
	 * Thread safe way to initialize and decrement a counter. 
	 * 
	 * @param key key where the data is stored
	 * @param inc value to set or increment by
	 * @param hashCode if not null, then the int hashcode to use
	 * @return value of incrementer
	 */
	public long addOrDecr( String key, long inc, Integer hashCode ) {
		boolean ret = set( "add", key, new Long( inc ), null, hashCode, true );

		if ( ret ) {
			return inc;
		}
		else {
			return incrdecr( "decr", key, inc, hashCode );
		}
	}

	/**
	 * Increment the value at the specified key by 1, and then return it.
	 *
	 * @param key key where the data is stored
	 * @return -1, if the key is not found, the value after incrementing otherwise
	 */
	public long incr( String key ) {
		return incrdecr( "incr", key, 1, null );
	}

	/** 
	 * Increment the value at the specified key by passed in val. 
	 * 
	 * @param key key where the data is stored
	 * @param inc how much to increment by
	 * @return -1, if the key is not found, the value after incrementing otherwise
	 */
	public long incr( String key, long inc ) {
		return incrdecr( "incr", key, inc, null );
	}

	/**
	 * Increment the value at the specified key by the specified increment, and then return it.
	 *
	 * @param key key where the data is stored
	 * @param inc how much to increment by
	 * @param hashCode if not null, then the int hashcode to use
	 * @return -1, if the key is not found, the value after incrementing otherwise
	 */
	public long incr( String key, long inc, Integer hashCode ) {
		return incrdecr( "incr", key, inc, hashCode );
	}
	
	/**
	 * Decrement the value at the specified key by 1, and then return it.
	 *
	 * @param key key where the data is stored
	 * @return -1, if the key is not found, the value after incrementing otherwise
	 */
	public long decr( String key ) {
		return incrdecr( "decr", key, 1, null );
	}

	/**
	 * Decrement the value at the specified key by passed in value, and then return it.
	 *
	 * @param key key where the data is stored
	 * @param inc how much to increment by
	 * @return -1, if the key is not found, the value after incrementing otherwise
	 */
	public long decr( String key, long inc ) {
		return incrdecr( "decr", key, inc, null );
	}

	/**
	 * Decrement the value at the specified key by the specified increment, and then return it.
	 *
	 * @param key key where the data is stored
	 * @param inc how much to increment by
	 * @param hashCode if not null, then the int hashcode to use
	 * @return -1, if the key is not found, the value after incrementing otherwise
	 */
	public long decr( String key, long inc, Integer hashCode ) {
		return incrdecr( "decr", key, inc, hashCode );
	}

	/** 
	 * Increments/decrements the value at the specified key by inc.
	 * 
	 *  Note that the server uses a 32-bit unsigned integer, and checks for<br/>
	 *  underflow. In the event of underflow, the result will be zero.  Because<br/>
	 *  Java lacks unsigned types, the value is returned as a 64-bit integer.<br/>
	 *  The server will only decrement a value if it already exists;<br/>
	 *  if a value is not found, -1 will be returned.
	 *
	 * @param cmdname increment/decrement
	 * @param key cache key
	 * @param inc amount to incr or decr
	 * @param hashCode if not null, then the int hashcode to use
	 * @return new value or -1 if not exist
	 */
	private long incrdecr( String cmdname, String key, long inc, Integer hashCode ) {

		if ( key == null ) {
			log.error( "null key for incrdecr()" );
			return -1;
		}

		try {
			key = sanitizeKey( key );
		}
		catch ( UnsupportedEncodingException e ) {

			// if we have an errorHandler, use its hook
			if ( errorHandler != null )
				errorHandler.handleErrorOnGet( this, e, key );

			log.error( "failed to sanitize your key!", e );
			return -1;
		}

		// get SockIO obj for given cache key
		if ( pool == null )
			pool = SockIOPool.getInstance( poolName );

		SockIOPool.SockIO sock = pool.getSock( key, hashCode );

		if ( sock == null )
			return -1;
		
		try {
			String cmd = String.format( "%s %s %d\r\n", cmdname, key, inc );
			log.debug( "++++ memcache incr/decr command: " + cmd );

			sock.write( cmd.getBytes() );
			sock.flush();

			// get result back
			String line = sock.readLine();

			if ( line.matches( "\\d+" ) ) {

				// return sock to pool and return result
				sock.close();
				try {
					return Long.parseLong( line );
				}
				catch ( Exception ex ) {

					// if we have an errorHandler, use its hook
					if ( errorHandler != null )
						errorHandler.handleErrorOnGet( this, ex, key );

					log.error( String.format( "Failed to parse Long value for key: %s", key ) );
				}
 			}
			else if ( NOTFOUND.equals( line ) ) {
				log.info( "++++ key not found to incr/decr for key: " + key );
			}
			else {
				log.error( "++++ error incr/decr key: " + key );
				log.error( "++++ server response: " + line );
			}
		}
		catch ( IOException e ) {

			// if we have an errorHandler, use its hook
			if ( errorHandler != null )
				errorHandler.handleErrorOnGet( this, e, key );

			// exception thrown
			log.error( "++++ exception thrown while writing bytes to server on incr/decr" );
			log.error( e.getMessage(), e );

			try {
				sock.trueClose();
			}
			catch ( IOException ioe ) {
				log.error( "++++ failed to close socket : " + sock.toString() );
			}

			sock = null;
		}
		
		if ( sock != null ) {
			sock.close();
			sock = null;
		}

		return -1;
	}

	/**
	 * Retrieve a key from the server, using a specific hash.
	 *
	 *  If the data was compressed or serialized when compressed, it will automatically<br/>
	 *  be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 *<br/>
	 *  Non-serialized data will be returned as a string, so explicit conversion to<br/>
	 *  numeric types will be necessary, if desired<br/>
	 *
	 * @param key key where data is stored
	 * @return the object that was previously stored, or null if it was not previously stored
	 */
	public Object get( String key ) {
		return get( key, null, false );
	}

	/** 
	 * Retrieve a key from the server, using a specific hash.
	 *
	 *  If the data was compressed or serialized when compressed, it will automatically<br/>
	 *  be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 *<br/>
	 *  Non-serialized data will be returned as a string, so explicit conversion to<br/>
	 *  numeric types will be necessary, if desired<br/>
	 *
	 * @param key key where data is stored
	 * @param hashCode if not null, then the int hashcode to use
	 * @return the object that was previously stored, or null if it was not previously stored
	 */
	public Object get( String key, Integer hashCode ) {
		return get( key, hashCode, false );
	}

	/**
	 * Retrieve a key from the server, using a specific hash.
	 *
	 *  If the data was compressed or serialized when compressed, it will automatically<br/>
	 *  be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 *<br/>
	 *  Non-serialized data will be returned as a string, so explicit conversion to<br/>
	 *  numeric types will be necessary, if desired<br/>
	 *
	 * @param key key where data is stored
	 * @param hashCode if not null, then the int hashcode to use
	 * @param asString if true, then return string val
	 * @return the object that was previously stored, or null if it was not previously stored
	 */
	public Object get( String key, Integer hashCode, boolean asString ) {

		if ( key == null ) {
			log.error( "key is null for get()" );
			return null;
		}

		try {
			key = sanitizeKey( key );
		}
		catch ( UnsupportedEncodingException e ) {

			// if we have an errorHandler, use its hook
			if ( errorHandler != null )
				errorHandler.handleErrorOnGet( this, e, key );

			log.error( "failed to sanitize your key!", e );
			return null;
		}

		// get SockIO obj using cache key
		if ( pool == null )
			pool = SockIOPool.getInstance( poolName );

		SockIOPool.SockIO sock = pool.getSock( key, hashCode );
	    
	    if ( sock == null )
			return null;

		Map<String,StringBuilder> cmdMap =
			new HashMap<String,StringBuilder>();

		cmdMap.put( sock.getHost(),
				new StringBuilder( String.format( "get %s", key ) ) );

		sock.close();

		// build empty map
		// and fill it from server
		Map<String,Object> hm =
			new HashMap<String,Object>();
		(new NIOLoader()).doMulti( asString, cmdMap, new String[] { key }, hm );

		// return the value for this key if we found it
		// else return null 
		return ( hm.containsKey( key ) ) ? hm.get( key ) : null;
	}

	/** 
	 * Retrieve multiple objects from the memcache.
	 *
	 *  This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
	 *  is more efficient.<br/>
	 *
	 * @param keys String array of keys to retrieve
	 * @return Object array ordered in same order as key array containing results
	 */
	public Object[] getMultiArray( String[] keys ) {
		return getMultiArray( keys, null, false );
	}

	/** 
	 * Retrieve multiple objects from the memcache.
	 *
	 *  This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
	 *  is more efficient.<br/>
	 *
	 * @param keys String array of keys to retrieve
	 * @param hashCodes if not null, then the Integer array of hashCodes
	 * @return Object array ordered in same order as key array containing results
	 */
	public Object[] getMultiArray( String[] keys, Integer[] hashCodes ) {
		return getMultiArray( keys, hashCodes, false );
	}

	/** 
	 * Retrieve multiple objects from the memcache.
	 *
	 *  This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
	 *  is more efficient.<br/>
	 *
	 * @param keys String array of keys to retrieve
	 * @param hashCodes if not null, then the Integer array of hashCodes
	 * @param asString if true, retrieve string vals
	 * @return Object array ordered in same order as key array containing results
	 */
	public Object[] getMultiArray( String[] keys, Integer[] hashCodes, boolean asString ) {

		Map<String,Object> data = getMulti( keys, hashCodes, asString );

		if ( data == null )
			return null;

		Object[] res = new Object[ keys.length ];
		for ( int i = 0; i < keys.length; i++ ) {
			res[i] = data.get( keys[i] );
		}

		return res;
	}

	/**
	 * Retrieve multiple objects from the memcache.
	 *
	 *  This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
	 *  is more efficient.<br/>
	 *
	 * @param keys String array of keys to retrieve
	 * @return a hashmap with entries for each key is found by the server,
	 *      keys that are not found are not entered into the hashmap, but attempting to
	 *      retrieve them from the hashmap gives you null.
	 */
	public Map<String,Object> getMulti( String[] keys ) {
		return getMulti( keys, null, false );
	}
    
	/**
	 * Retrieve multiple keys from the memcache.
	 *
	 *  This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
	 *  is more efficient.<br/>
	 *
	 * @param keys keys to retrieve
	 * @param hashCodes if not null, then the Integer array of hashCodes
	 * @return a hashmap with entries for each key is found by the server,
	 *      keys that are not found are not entered into the hashmap, but attempting to
	 *      retrieve them from the hashmap gives you null.
	 */
	public Map<String,Object> getMulti( String[] keys, Integer[] hashCodes ) {
		return getMulti( keys, hashCodes, false );
	}

	/**
	 * Retrieve multiple keys from the memcache.
	 *
	 *  This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
	 *  is more efficient.<br/>
	 *
	 * @param keys keys to retrieve
	 * @param hashCodes if not null, then the Integer array of hashCodes
	 * @param asString if true then retrieve using String val
	 * @return a hashmap with entries for each key is found by the server,
	 *      keys that are not found are not entered into the hashmap, but attempting to
	 *      retrieve them from the hashmap gives you null.
	 */
	public Map<String,Object> getMulti( String[] keys, Integer[] hashCodes, boolean asString ) {

		if ( keys == null || keys.length == 0 ) {
			log.error( "missing keys for getMulti()" );
			return null;
		}

		Map<String,StringBuilder> cmdMap =
			new HashMap<String,StringBuilder>();

		for ( int i = 0; i < keys.length; ++i ) {

			String key = keys[i];
			if ( key == null ) {
				log.error( "null key, so skipping" );
				continue;
			}

			Integer hash = null;
			if ( hashCodes != null && hashCodes.length > i )
				hash = hashCodes[ i ];

			String cleanKey = key;
			try {
				cleanKey = sanitizeKey( key );
			}
			catch ( UnsupportedEncodingException e ) {

				// if we have an errorHandler, use its hook
				if ( errorHandler != null )
					errorHandler.handleErrorOnGet( this, e, key );

				log.error( "failed to sanitize your key!", e );
				continue;
			}

			// get SockIO obj from cache key
			if ( pool == null )
				pool = SockIOPool.getInstance( poolName );

			SockIOPool.SockIO sock = pool.getSock( cleanKey, hash );

			if ( sock == null )
				continue;

			// store in map and list if not already
			if ( !cmdMap.containsKey( sock.getHost() ) )
				cmdMap.put( sock.getHost(), new StringBuilder( "get" ) );

			cmdMap.get( sock.getHost() ).append( " " + cleanKey );

			// return to pool
			sock.close();
		}
		
		log.info( "multi get socket count : " + cmdMap.size() );

		// now query memcache
		Map<String,Object> ret =
			new HashMap<String,Object>( keys.length );

		// now use new NIO implementation
		(new NIOLoader()).doMulti( asString, cmdMap, keys, ret );

		// fix the return array in case we had to rewrite any of the keys
		for ( String key : keys ) {

			String cleanKey = key;
			try {
				cleanKey = sanitizeKey( key );
			}
			catch ( UnsupportedEncodingException e ) {

				// if we have an errorHandler, use its hook
				if ( errorHandler != null )
					errorHandler.handleErrorOnGet( this, e, key );

				log.error( "failed to sanitize your key!", e );
				continue;
			}

			if ( ! key.equals( cleanKey ) && ret.containsKey( cleanKey ) ) {
				ret.put( key, ret.get( cleanKey ) );
				ret.remove( cleanKey );
			}

			// backfill missing keys w/ null value
			if ( ! ret.containsKey( key ) )
				ret.put( key, null );
		}

		log.debug( "++++ memcache: got back " + ret.size() + " results" );
		return ret;
	}

	/** 
	 * This method loads the data from cache into a Map.
	 *
	 * Pass a SockIO object which is ready to receive data and a HashMap<br/>
	 * to store the results.
	 * 
	 * @param sock socket waiting to pass back data
	 * @param hm hashmap to store data into
	 * @param asString if true, and if we are using NativehHandler, return string val
	 * @throws IOException if io exception happens while reading from socket
	 */
	private void loadMulti( LineInputStream input, Map<String,Object> hm, boolean asString ) throws IOException {

		while ( true ) {
			String line = input.readLine();
			log.debug( "++++ line: " + line );

			if ( line.startsWith( VALUE ) ) {
				String[] info = line.split(" ");
				String key    = info[1];
				int flag      = Integer.parseInt( info[2] );
				int length    = Integer.parseInt( info[3] );

				log.debug( "++++ key: " + key );
				log.debug( "++++ flags: " + flag );
				log.debug( "++++ length: " + length );
				
				// read obj into buffer
				byte[] buf = new byte[length];
				input.read( buf );
				input.clearEOL();

				// ready object
				Object o;
				
				// check for compression
				if ( (flag & F_COMPRESSED) == F_COMPRESSED ) {
					try {
						// read the input stream, and write to a byte array output stream since
						// we have to read into a byte array, but we don't know how large it
						// will need to be, and we don't want to resize it a bunch
						GZIPInputStream gzi = new GZIPInputStream( new ByteArrayInputStream( buf ) );
						ByteArrayOutputStream bos = new ByteArrayOutputStream( buf.length );
						
						int count;
						byte[] tmp = new byte[2048];
						while ( (count = gzi.read(tmp)) != -1 ) {
							bos.write( tmp, 0, count );
						}

						// store uncompressed back to buffer
						buf = bos.toByteArray();
						gzi.close();
					}
					catch ( IOException e ) {

						// if we have an errorHandler, use its hook
						if ( errorHandler != null )
							errorHandler.handleErrorOnGet( this, e, key );

						log.error( "++++ IOException thrown while trying to uncompress input stream for key: " + key );
						log.error( e.getMessage(), e );
						throw new NestedIOException( "++++ IOException thrown while trying to uncompress input stream for key: " + key, e );
					}
				}

				// we can only take out serialized objects
				if ( ( flag & F_SERIALIZED ) != F_SERIALIZED ) {
					if ( primitiveAsString || asString ) {
						// pulling out string value
						log.info( "++++ retrieving object and stuffing into a string." );
						o = new String( buf, defaultEncoding );
					}
					else {
						// decoding object
						try {
							o = NativeHandler.decode( buf, flag );    
						}
						catch ( Exception e ) {

							// if we have an errorHandler, use its hook
							if ( errorHandler != null )
								errorHandler.handleErrorOnGet( this, e, key );

							log.error( "++++ Exception thrown while trying to deserialize for key: " + key, e );
							throw new NestedIOException( e );
						}
					}
				}
				else {
					// deserialize if the data is serialized
					ContextObjectInputStream ois =
						new ContextObjectInputStream( new ByteArrayInputStream( buf ), classLoader );
					try {
						o = ois.readObject();
						log.info( "++++ deserializing " + o.getClass() );
					}
					catch ( ClassNotFoundException e ) {

						// if we have an errorHandler, use its hook
						if ( errorHandler != null )
							errorHandler.handleErrorOnGet( this, e, key );

						log.error( "++++ ClassNotFoundException thrown while trying to deserialize for key: " + key, e );
						throw new NestedIOException( "+++ failed while trying to deserialize for key: " + key, e );
					}
				}

				// store the object into the cache
				hm.put( key, o );
			}
			else if ( END.equals( line ) ) {
				log.debug( "++++ finished reading from cache server" );
				break;
			}
		}
	}

	private String sanitizeKey( String key ) throws UnsupportedEncodingException {
		return ( sanitizeKeys ) ? URLEncoder.encode( key, "UTF-8" ) : key;
	}

	/** 
	 * Invalidates the entire cache.
	 *
	 * Will return true only if succeeds in clearing all servers.
	 * 
	 * @return success true/false
	 */
	public boolean flushAll() {
		return flushAll( null );
	}

	/** 
	 * Invalidates the entire cache.
	 *
	 * Will return true only if succeeds in clearing all servers.
	 * If pass in null, then will try to flush all servers.
	 * 
	 * @param servers optional array of host(s) to flush (host:port)
	 * @return success true/false
	 */
	public boolean flushAll( String[] servers ) {

		// get SockIOPool instance
		if ( pool == null )
			pool = SockIOPool.getInstance( poolName );

		// return false if unable to get SockIO obj
		if ( pool == null ) {
			log.error( "++++ unable to get SockIOPool instance" );
			return false;
		}

		// get all servers and iterate over them
		servers = ( servers == null )
			? pool.getServers()
			: servers;

		// if no servers, then return early
		if ( servers == null || servers.length <= 0 ) {
			log.error( "++++ no servers to flush" );
			return false;
		}

		boolean success = true;

		for ( int i = 0; i < servers.length; i++ ) {

			SockIOPool.SockIO sock = pool.getConnection( servers[i] );
			if ( sock == null ) {
				log.error( "++++ unable to get connection to : " + servers[i] );
				success = false;
				continue;
			}

			// build command
			String command = "flush_all\r\n";

			try {
				sock.write( command.getBytes() );
				sock.flush();

				// if we get appropriate response back, then we return true
				String line = sock.readLine();
				success = ( OK.equals( line ) )
					? success && true
					: false;
			}
			catch ( IOException e ) {

				// if we have an errorHandler, use its hook
				if ( errorHandler != null )
					errorHandler.handleErrorOnFlush( this, e );

				// exception thrown
				log.error( "++++ exception thrown while writing bytes to server on flushAll" );
				log.error( e.getMessage(), e );

				try {
					sock.trueClose();
				}
				catch ( IOException ioe ) {
					log.error( "++++ failed to close socket : " + sock.toString() );
				}

				success = false;
				sock = null;
			}

			if ( sock != null ) {
				sock.close();
				sock = null;
			}
		}

		return success;
	}

	/** 
	 * Retrieves stats for all servers.
	 *
	 * Returns a map keyed on the servername.
	 * The value is another map which contains stats
	 * with stat name as key and value as value.
	 * 
	 * @return Stats map
	 */
	public Map stats() {
		return stats( null );
	}

	/** 
	 * Retrieves stats for passed in servers (or all servers).
	 *
	 * Returns a map keyed on the servername.
	 * The value is another map which contains stats
	 * with stat name as key and value as value.
	 * 
	 * @param servers string array of servers to retrieve stats from, or all if this is null	 
	 * @return Stats map
	 */
	public Map stats( String[] servers ) {
		return stats( servers, "stats\r\n", STATS );
	}	

	/** 
	 * Retrieves stats items for all servers.
	 *
	 * Returns a map keyed on the servername.
	 * The value is another map which contains item stats
	 * with itemname:number:field as key and value as value.
	 * 
	 * @return Stats map
	 */
	public Map statsItems() {
		return statsItems( null );
	}
	
	/** 
	 * Retrieves stats for passed in servers (or all servers).
	 *
	 * Returns a map keyed on the servername.
	 * The value is another map which contains item stats
	 * with itemname:number:field as key and value as value.
	 * 
	 * @param servers string array of servers to retrieve stats from, or all if this is null
	 * @return Stats map
	 */
	public Map statsItems( String[] servers ) {
		return stats( servers, "stats items\r\n", STATS );
	}
	
	/** 
	 * Retrieves stats items for all servers.
	 *
	 * Returns a map keyed on the servername.
	 * The value is another map which contains slabs stats
	 * with slabnumber:field as key and value as value.
	 * 
	 * @return Stats map
	 */
	public Map statsSlabs() {
		return statsSlabs( null );
	}
	
	/** 
	 * Retrieves stats for passed in servers (or all servers).
	 *
	 * Returns a map keyed on the servername.
	 * The value is another map which contains slabs stats
	 * with slabnumber:field as key and value as value.
	 * 
	 * @param servers string array of servers to retrieve stats from, or all if this is null
	 * @return Stats map
	 */
	public Map statsSlabs( String[] servers ) {
		return stats( servers, "stats slabs\r\n", STATS );
	}
	
	/** 
	 * Retrieves items cachedump for all servers.
	 *
	 * Returns a map keyed on the servername.
	 * The value is another map which contains cachedump stats
	 * with the cachekey as key and byte size and unix timestamp as value.
	 * 
	 * @param slabNumber the item number of the cache dump
	 * @return Stats map
	 */
	public Map statsCacheDump( int slabNumber, int limit ) {
		return statsCacheDump( null, slabNumber, limit );
	}
	
	/** 
	 * Retrieves stats for passed in servers (or all servers).
	 *
	 * Returns a map keyed on the servername.
	 * The value is another map which contains cachedump stats
	 * with the cachekey as key and byte size and unix timestamp as value.
	 * 
	 * @param servers string array of servers to retrieve stats from, or all if this is null
	 * @param slabNumber the item number of the cache dump
	 * @return Stats map
	 */
	public Map statsCacheDump( String[] servers, int slabNumber, int limit ) {
		return stats( servers, String.format( "stats cachedump %d %d\r\n", slabNumber, limit ), ITEM );
	}
		
	private Map stats( String[] servers, String command, String lineStart ) {

		if ( command == null || command.trim().equals( "" ) ) {
			log.error( "++++ invalid / missing command for stats()" );
			return null;
		}

		// get SockIOPool instance
		if ( pool == null )
			pool = SockIOPool.getInstance( poolName );

		// return false if unable to get SockIO obj
		if ( pool == null ) {
			log.error( "++++ unable to get SockIOPool instance" );
			return null;
		}

		// get all servers and iterate over them
		servers = (servers == null)
			? pool.getServers()
			: servers;

		// if no servers, then return early
		if ( servers == null || servers.length <= 0 ) {
			log.error( "++++ no servers to check stats" );
			return null;
		}

		// array of stats Maps
		Map<String,Map> statsMaps =
			new HashMap<String,Map>();

		for ( int i = 0; i < servers.length; i++ ) {

			SockIOPool.SockIO sock = pool.getConnection( servers[i] );
			if ( sock == null ) {
				log.error( "++++ unable to get connection to : " + servers[i] );
				continue;
			}

			// build command
			try {
				sock.write( command.getBytes() );
				sock.flush();

				// map to hold key value pairs
				Map<String,String> stats = new HashMap<String,String>();

				// loop over results
				while ( true ) {
					String line = sock.readLine();
					log.debug( "++++ line: " + line );

					if ( line.startsWith( lineStart ) ) {
						String[] info = line.split( " ", 3 );						
						String key    = info[1];
						String value  = info[2];

						log.debug( "++++ key  : " + key );
						log.debug( "++++ value: " + value );

						stats.put( key, value );
					}
					else if ( END.equals( line ) ) {
						// finish when we get end from server
						log.debug( "++++ finished reading from cache server" );
						break;
					}
					else if ( line.startsWith( ERROR ) || line.startsWith( CLIENT_ERROR ) || line.startsWith( SERVER_ERROR ) ) {
						log.error( "++++ failed to query stats" );
						log.error( "++++ server response: " + line );
						break;
					}

					statsMaps.put( servers[i], stats );
				}
			}
			catch ( IOException e ) {

				// if we have an errorHandler, use its hook
				if ( errorHandler != null )
					errorHandler.handleErrorOnStats( this, e );

				// exception thrown
				log.error( "++++ exception thrown while writing bytes to server on stats" );
				log.error( e.getMessage(), e );

				try {
					sock.trueClose();
				}
				catch ( IOException ioe ) {
					log.error( "++++ failed to close socket : " + sock.toString() );
				}

				sock = null;
			}

			if ( sock != null ) {
				sock.close();
				sock = null;
			}
		}

		return statsMaps;
	}

	protected final class NIOLoader {
		protected Selector selector;
		protected int numConns = 0;
		protected Connection[] conns;

		private final class Connection {
		
			public List<ByteBuffer> incoming = new ArrayList<ByteBuffer>();
			public ByteBuffer outgoing;
			public SockIOPool.SockIO sock;
			public SocketChannel channel;
			private boolean isDone = false;
			
			public Connection( SockIOPool.SockIO sock, StringBuilder request ) throws IOException {
				log.debug( "setting up connection to "+sock.getHost() );
				
				this.sock = sock;
				outgoing = ByteBuffer.wrap( request.append( "\r\n" ).toString().getBytes() );
				
				channel = sock.getChannel();
				channel.configureBlocking( false );
				channel.register( selector, SelectionKey.OP_WRITE, this );
			}
			
			public void close() {
				try {
					if ( isDone ) {
						// turn off non-blocking IO and return to pool
						if ( log.isDebugEnabled() )
							log.debug( "++++ gracefully closing connection to "+sock.getHost() );
						
						channel.configureBlocking( true );
						sock.close();
						return;
					}
				}
				catch ( IOException e ) {
					log.warn( "++++ memcache: unexpected error closing normally" );
				}
				
				try {
					if ( log.isDebugEnabled() )
						log.debug("forcefully closing connection to "+sock.getHost());

					channel.close();
					sock.trueClose();
				}
				catch ( IOException ignoreMe ) { }
			}
			
			public boolean isDone() {
				// if we know we're done, just say so
				if ( isDone )         
					return true;
				
				// else find out the hard way
				int maxBuf = incoming.size()-1;
				int strPos = B_END.length-1;

				// Need to check if last bytes are:
				//   - END\r\n
				//   - NOT_FOUND\r\n
				//   - DELETED\r\n
				
				int bi = maxBuf;
				while ( bi >= 0 && strPos >= 0 ) {
					ByteBuffer buf = incoming.get( bi );
					int pos = buf.position()-1;
					while ( pos >= 0 && strPos >= 0 ) {
					    if ( buf.get( pos-- ) != B_END[strPos--] )
							return false;
					}

					bi--;
				}
				
				isDone = strPos < 0;
				return isDone;
			}
			
			public ByteBuffer getBuffer() {
				int last = incoming.size()-1;
				if ( last >= 0 && incoming.get( last ).hasRemaining() ) {
					return incoming.get( last );
				}
				else {
					ByteBuffer newBuf = ByteBuffer.allocate( 8192 );
					incoming.add( newBuf );
					return newBuf;
				}
			}
			
			public String toString() {
				return "Connection to " + sock.getHost() + " with " + incoming.size() + " bufs; done is " + isDone;
			}
		}
		
		public void doMulti( boolean asString, Map<String, StringBuilder> sockKeys, String[] keys, Map<String, Object> ret ) {
		
			long timeRemaining = 0;
			try {
				selector = Selector.open();
				
				// get the sockets, flip them to non-blocking, and set up data
				// structures
				conns = new Connection[sockKeys.keySet().size()];
				numConns = 0;
				for ( Iterator<String> i = sockKeys.keySet().iterator(); i.hasNext(); ) {
					// get SockIO obj from hostname
					String host = i.next();

					if ( pool == null )
						pool = SockIOPool.getInstance( poolName );

					SockIOPool.SockIO sock = pool.getConnection( host );
					conns[numConns++] = new Connection( sock, sockKeys.get( host ) );
				}
				
				// the main select loop; ends when
				// 1) we've received data from all the servers, or
				// 2) we time out
				long startTime = System.currentTimeMillis();

				if ( pool == null )
					pool = SockIOPool.getInstance( poolName );

				long timeout = pool.getMaxBusy();
				timeRemaining = timeout;
				
				while ( numConns > 0 && timeRemaining > 0 ) {
					int n = selector.select( 5000 );
					if ( n > 0 ) {
					    // we've got some activity; handle it
					    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
					    while ( it.hasNext() ) {
					        SelectionKey key = it.next();
					        it.remove();
					        handleKey( key );
					    }
					}
					else {
					    // timeout likely... better check
						// TODO:  This seems like a problem area that we need to figure out how to handle.
						log.error( "selector timed out waiting for activity" );
					}
					
					timeRemaining = timeout - (System.currentTimeMillis() - startTime);
				}
			}
			catch ( IOException e ) {
				// errors can happen just about anywhere above, from
				// connection setup to any of the mechanics
				handleError( e, keys );
				return;
			}
			finally {
				if ( log.isDebugEnabled() )
					log.debug( "Disconnecting; numConns=" + numConns + "  timeRemaining=" + timeRemaining );
				
				// run through our conns and either return them to the pool
				// or forcibly close them
				try {
					selector.close();
				}
				catch ( IOException ignoreMe ) { }
				
				for ( Connection c : conns ) {
					if ( c != null )
						c.close();
				}
			}
		
			// Done!  Build the list of results and return them.  If we get
			// here by a timeout, then some of the connections are probably
			// not done.  But we'll return what we've got...
			for ( Connection c : conns ) {
				try {
					if ( c.incoming.size() > 0 && c.isDone() )
						loadMulti( new ByteBufArrayInputStream( c.incoming ), ret, asString );
				}
				catch ( Exception e ) {
					// shouldn't happen; we have all the data already
					log.warn( "Caught the aforementioned exception on "+c );
				}
			}
		}
		
		private void handleError( Throwable e, String[] keys ) {
		    // if we have an errorHandler, use its hook
		    if ( errorHandler != null )
		        errorHandler.handleErrorOnGet( MemCachedClient.this, e, keys );
		
		    // exception thrown
		    log.error( "++++ exception thrown while getting from cache on getMulti" );
		    log.error( e.getMessage() );
		}
		
		private void handleKey( SelectionKey key ) throws IOException {
			if ( log.isDebugEnabled() )
				log.debug( "handling selector op " + key.readyOps() + " for key " + key );
			
			if ( key.isReadable() )
				readResponse( key );
			else if ( key.isWritable() )
				writeRequest( key );
		}
		
		public void writeRequest( SelectionKey key ) throws IOException {
			ByteBuffer buf = ((Connection) key.attachment()).outgoing;
			SocketChannel sc = (SocketChannel)key.channel();
			
			if ( buf.hasRemaining() ) {
				if ( log.isDebugEnabled() )
				    log.debug( "writing " + buf.remaining() + "B to " + ((SocketChannel) key.channel()).socket().getInetAddress() );

				sc.write( buf );
			}
			
			if ( !buf.hasRemaining() ) {
			    if ( log.isDebugEnabled() )
			        log.debug( "switching to read mode for server " + ((SocketChannel)key.channel()).socket().getInetAddress() );

				key.interestOps( SelectionKey.OP_READ );
			}
		}
		
		public void readResponse( SelectionKey key ) throws IOException {
			Connection conn = (Connection)key.attachment();
			InetAddress remote = conn.channel.socket().getInetAddress();
			
			ByteBuffer buf = conn.getBuffer();
			int count = conn.channel.read( buf );
			if ( count > 0 ) {
				if ( log.isDebugEnabled() )
					log.debug( "read  " + count + " from " + remote );
				
				if ( conn.isDone() ) {
					log.debug( "connection done to  " + remote );
					numConns--;
					return;
				}
			}
		}
	}
}
