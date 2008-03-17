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

import java.util.Date;
import org.apache.log4j.Logger;

/**
 * Handle encoding standard Java types directly which can result in significant
 * memory savings:
 * 
 * Currently the Memcached driver for Java supports the setSerialize() option.
 * This can increase performance in some situations but has a few issues:
 * 
 * Code that performs class casting will throw ClassCastExceptions when
 * setSerialize is enabled. For example:
 * 
 *     mc.set( "foo", new Integer( 1 ) ); Integer output = (Integer)mc.get("foo");
 * 
 * Will work just file when setSerialize is true but when its false will just throw
 * a ClassCastException.
 * 
 * Also internally it doesn't support Boolean and since toString is called wastes a
 * lot of memory and causes additional performance issue.  For example an Integer
 * can take anywhere from 1 byte to 10 bytes.
 * 
 * Due to the way the memcached slab allocator works it seems like a LOT of wasted
 * memory to store primitive types as serialized objects (from a performance and
 * memory perspective).  In our applications we have millions of small objects and
 * wasted memory would become a big problem.
 * 
 * For example a Serialized Boolean takes 47 bytes which means it will fit into the
 * 64byte LRU.  Using 1 byte means it will fit into the 8 byte LRU thus saving 8x
 * the memory.  This also saves the CPU performance since we don't have to
 * serialize bytes back and forth and we can compute the byte[] value directly.
 * 
 * One problem would be when the user calls get() because doing so would require
 * the app to know the type of the object stored as a bytearray inside memcached
 * (since the user will probably cast).
 * 
 * If we assume the basic types are interned we could use the first byte as the
 * type with the remaining bytes as the value.  Then on get() we could read the
 * first byte to determine the type and then construct the correct object for it.
 * This would prevent the ClassCastException I talked about above.
 * 
 * We could remove the setSerialize() option and just assume that standard VM types
 * are always internd in this manner.
 * 
 * mc.set( "foo", new Boolean.TRUE ); Boolean b = (Boolean)mc.get( "foo" );
 * 
 * And the type casts would work because internally we would create a new Boolean
 * to return back to the client.
 * 
 * This would reduce memory footprint and allow for a virtual implementation of the
 * Externalizable interface which is much faster than Serialzation.
 * 
 * Currently the memory improvements would be:
 * 
 * java.lang.Boolean - 8x performance improvement (now just two bytes)
 * java.lang.Integer - 16x performance improvement (now just 5 bytes)
 * 
 * Most of the other primitive types would benefit from this optimization.
 * java.lang.Character being another obvious example.
 * 
 * I know it seems like I'm being really picky here but for our application I'd
 * save 1G of memory right off the bat.  We'd go down from 1.152G of memory used
 * down to 144M of memory used which is much better IMO.
 * 
 * http://java.sun.com/docs/books/tutorial/native1.1/integrating/types.html
 *
 * @author <a href="mailto:burton@peerfear.org">Kevin A. Burton</a>
 * @author Greg Whalin <greg@meetup.com> 
 */
public class NativeHandler {

	// logger
	private static Logger log =
		Logger.getLogger( NativeHandler.class.getName() );

	/** 
	 * Detemine of object can be natively serialized by this class. 
	 * 
	 * @param value Object to test.
	 * @return true/false
	 */
	public static boolean isHandled( Object value ) {

		return (
			value instanceof Byte            ||
			value instanceof Boolean         ||
			value instanceof Integer         ||
			value instanceof Long            ||
			value instanceof Character       ||
			value instanceof String          ||
			value instanceof StringBuffer    ||
			value instanceof Float           ||
			value instanceof Short           ||
			value instanceof Double          ||
			value instanceof Date            ||
			value instanceof StringBuilder   ||
			value instanceof byte[]
			)
		? true
		: false;
    }

	/** 
	 * Returns the flag for marking the type of the byte array. 
	 * 
	 * @param value Object we are storing.
	 * @return int marker
	 */
	public static int getMarkerFlag( Object value ) {

		if ( value instanceof Byte )
			return MemCachedClient.MARKER_BYTE;
		
		if ( value instanceof Boolean )
			return MemCachedClient.MARKER_BOOLEAN;
		
		if ( value instanceof Integer ) 
			return MemCachedClient.MARKER_INTEGER;
		
		if ( value instanceof Long )
			return MemCachedClient.MARKER_LONG;

		if ( value instanceof Character )
			return MemCachedClient.MARKER_CHARACTER;
		
		if ( value instanceof String )
			return MemCachedClient.MARKER_STRING;
		
		if ( value instanceof StringBuffer )
			return MemCachedClient.MARKER_STRINGBUFFER;
		
		if ( value instanceof Float )
			return MemCachedClient.MARKER_FLOAT;
		
		if ( value instanceof Short )
			return MemCachedClient.MARKER_SHORT;
		
		if ( value instanceof Double )
			return MemCachedClient.MARKER_DOUBLE;
		
		if ( value instanceof Date )
			return MemCachedClient.MARKER_DATE;
		
		if ( value instanceof StringBuilder )
			return MemCachedClient.MARKER_STRINGBUILDER;
		
		if ( value instanceof byte[] )
			return MemCachedClient.MARKER_BYTEARR;
		
		return -1;
	}

	/** 
	 * Encodes supported types 
	 * 
	 * @param value Object to encode.
	 * @return byte array
	 *
	 * @throws Exception If fail to encode.
	 */
	public static byte[] encode( Object value ) throws Exception {
	
		if ( value instanceof Byte )
			return encode( (Byte)value );
		
		if ( value instanceof Boolean )
			return encode( (Boolean)value );
		
		if ( value instanceof Integer ) 
			return encode( ((Integer)value).intValue() );
		
		if ( value instanceof Long )
			return encode( ((Long)value).longValue() );
		
		if ( value instanceof Character )
			return encode( (Character)value );
		
		if ( value instanceof String )
			return encode( (String)value );
		
		if ( value instanceof StringBuffer )
			return encode( (StringBuffer)value );
		
		if ( value instanceof Float )
			return encode( ((Float)value).floatValue() );
		
		if ( value instanceof Short )
			return encode( (Short)value );
		
		if ( value instanceof Double )
			return encode( ((Double)value).doubleValue() );
		
		if ( value instanceof Date )
			return encode( (Date)value);
		
		if ( value instanceof StringBuilder )
			return encode( (StringBuilder)value );
		
		if ( value instanceof byte[] )
			return encode( (byte[])value );
		
		return null;
	}

	protected static byte[] encode( Byte value ) {
		byte[] b = new byte[1];
		b[0] = value.byteValue();
		return b;
	}

	protected static byte[] encode( Boolean value ) {
		byte[] b = new byte[1];

		if ( value.booleanValue() )
			b[0] = 1;
		else
			b[0] = 0;

		return b;
	}

	protected static byte[] encode( int value ) {
		return getBytes( value );
	}
	
	protected static byte[] encode( long value ) throws Exception {
		return getBytes( value );
	}
	
	protected static byte[] encode( Date value ) {
		return getBytes( value.getTime() );
	}
	
	protected static byte[] encode( Character value ) {
		return encode( value.charValue() );
	}
	
	protected static byte[] encode( String value ) throws Exception {
		return value.getBytes( "UTF-8" );
	}
	
	protected static byte[] encode( StringBuffer value ) throws Exception {
		return encode( value.toString() );
	}
	
	protected static byte[] encode( float value ) throws Exception {
		return encode( (int)Float.floatToIntBits( value ) );
	}
	
	protected static byte[] encode( Short value ) throws Exception {
		return encode( (int)value.shortValue() );
	}
	
	protected static byte[] encode( double value ) throws Exception {
		return encode( (long)Double.doubleToLongBits( value ) );
	}
	
	protected static byte[] encode( StringBuilder value ) throws Exception {
		return encode( value.toString() );
	}
	
	protected static byte[] encode( byte[] value ) {
		return value;
	}

	protected static byte[] getBytes( long value ) {
		byte[] b = new byte[8];
		b[0] = (byte)((value >> 56) & 0xFF);
		b[1] = (byte)((value >> 48) & 0xFF);
		b[2] = (byte)((value >> 40) & 0xFF);
		b[3] = (byte)((value >> 32) & 0xFF);
		b[4] = (byte)((value >> 24) & 0xFF);
		b[5] = (byte)((value >> 16) & 0xFF);
		b[6] = (byte)((value >> 8) & 0xFF);
		b[7] = (byte)((value >> 0) & 0xFF);
		return b;
	}
	
	protected static byte[] getBytes( int value ) {
		byte[] b = new byte[4];
		b[0] = (byte)((value >> 24) & 0xFF);
		b[1] = (byte)((value >> 16) & 0xFF);
		b[2] = (byte)((value >> 8) & 0xFF);
		b[3] = (byte)((value >> 0) & 0xFF);
		return b;
	}
	
	/** 
	 * Decodes byte array using memcache flag to determine type.
	 * 
	 * @param b 
	 * @param marker 
	 * @return 
	 * @throws Exception 
	 */
	public static Object decode( byte[] b, int flag ) throws Exception {

		if ( b.length < 1 )
			return null;

		
		if ( ( flag & MemCachedClient.MARKER_BYTE ) == MemCachedClient.MARKER_BYTE )
			return decodeByte( b );
		
		if ( ( flag & MemCachedClient.MARKER_BOOLEAN ) == MemCachedClient.MARKER_BOOLEAN )
			return decodeBoolean( b );
		
		if ( ( flag & MemCachedClient.MARKER_INTEGER ) == MemCachedClient.MARKER_INTEGER )
			return decodeInteger( b );
		
		if ( ( flag & MemCachedClient.MARKER_LONG ) == MemCachedClient.MARKER_LONG )
			return decodeLong( b );
		
		if ( ( flag & MemCachedClient.MARKER_CHARACTER ) == MemCachedClient.MARKER_CHARACTER )
			return decodeCharacter( b );
		
		if ( ( flag & MemCachedClient.MARKER_STRING ) == MemCachedClient.MARKER_STRING )
			return decodeString( b );
		
		if ( ( flag & MemCachedClient.MARKER_STRINGBUFFER ) == MemCachedClient.MARKER_STRINGBUFFER )
			return decodeStringBuffer( b );
		
		if ( ( flag & MemCachedClient.MARKER_FLOAT ) == MemCachedClient.MARKER_FLOAT )
			return decodeFloat( b );
		
		if ( ( flag & MemCachedClient.MARKER_SHORT ) == MemCachedClient.MARKER_SHORT )
			return decodeShort( b );
		
		if ( ( flag & MemCachedClient.MARKER_DOUBLE ) == MemCachedClient.MARKER_DOUBLE )
			return decodeDouble( b );
		
		if ( ( flag & MemCachedClient.MARKER_DATE ) == MemCachedClient.MARKER_DATE )
			return decodeDate( b );
		
		if ( ( flag & MemCachedClient.MARKER_STRINGBUILDER ) == MemCachedClient.MARKER_STRINGBUILDER )
			return decodeStringBuilder( b );
		
		if ( ( flag & MemCachedClient.MARKER_BYTEARR ) == MemCachedClient.MARKER_BYTEARR )
			return decodeByteArr( b );
		
		return null;
	}
	
	// decode methods
	protected static Byte decodeByte( byte[] b ) {
		return new Byte( b[0] );
	}
	
	protected static Boolean decodeBoolean( byte[] b ) {
		boolean value = b[0] == 1;
		return ( value ) ? Boolean.TRUE : Boolean.FALSE;
	}
	
	protected static Integer decodeInteger( byte[] b ) {
		return new Integer( toInt( b ) );
	}
	
	protected static Long decodeLong( byte[] b ) throws Exception {
		return new Long( toLong( b ) );
	}
	
	protected static Character decodeCharacter( byte[] b ) {
		return new Character( (char)decodeInteger( b ).intValue() );
	}
	
	protected static String decodeString( byte[] b ) throws Exception {
		return new String( b, "UTF-8" );
	}
	
	protected static StringBuffer decodeStringBuffer( byte[] b ) throws Exception {
		return new StringBuffer( decodeString( b ) );
	}
	
	protected static Float decodeFloat( byte[] b ) throws Exception {
		Integer l = decodeInteger( b );
		return new Float( Float.intBitsToFloat( l.intValue() ) );
	}
	
	protected static Short decodeShort( byte[] b ) throws Exception {
		return new Short( (short)decodeInteger( b ).intValue() );
	}
	
	protected static Double decodeDouble( byte[] b ) throws Exception {
		Long l = decodeLong( b );
		return new Double( Double.longBitsToDouble( l.longValue() ) );
	}
	
	protected static Date decodeDate( byte[] b ) {
		return new Date( toLong( b ) );
	}
	
	protected static StringBuilder decodeStringBuilder( byte[] b ) throws Exception {
		return new StringBuilder( decodeString( b ) );
	}
	
	protected static byte[] decodeByteArr( byte[] b ) {
		return b;
	}
	
	/** 
	 * This works by taking each of the bit patterns and converting them to
	 * ints taking into account 2s complement and then adding them..
	 * 
	 * @param b 
	 * @return 
	 */
	protected static int toInt( byte[] b ) {
		return (((((int) b[3]) & 0xFF) << 32) +
			((((int) b[2]) & 0xFF) << 40) +
			((((int) b[1]) & 0xFF) << 48) +
			((((int) b[0]) & 0xFF) << 56));
	}    
	
	protected static long toLong( byte[] b ) {
		return ((((long) b[7]) & 0xFF) +
			((((long) b[6]) & 0xFF) << 8) +
			((((long) b[5]) & 0xFF) << 16) +
			((((long) b[4]) & 0xFF) << 24) +
			((((long) b[3]) & 0xFF) << 32) +
			((((long) b[2]) & 0xFF) << 40) +
			((((long) b[1]) & 0xFF) << 48) +
			((((long) b[0]) & 0xFF) << 56));
	}    
}
