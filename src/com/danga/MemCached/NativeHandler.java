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
 * @author Kevin A. Burton <burton@peerfear.org> 
 * @author Greg Whalin <greg@meetup.com> 
 * @version 1.5.2
 */
package com.danga.MemCached;

import java.util.Date;

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
 *
 * @author <a href="mailto:burton@peerfear.org">Kevin A. Burton</a>
 * @author Greg Whalin <greg@meetup.com> 
 */
public class NativeHandler {

    // http://java.sun.com/docs/books/tutorial/native1.1/integrating/types.html

    //FIXME: what about other common types?  Also what about
    //Collectiosn of native types?  I could reconstruct these on the remote end
    //if necessary.  Though I'm not sure of the performance advantage here.
    
    public static final int MARKER_BYTE             = 1;
    public static final int MARKER_BOOLEAN          = 2;
    public static final int MARKER_INTEGER          = 3;
    public static final int MARKER_LONG             = 4;
    public static final int MARKER_CHARACTER        = 5;
    public static final int MARKER_STRING           = 6;
    public static final int MARKER_STRINGBUFFER     = 7;
    public static final int MARKER_FLOAT            = 8;
    public static final int MARKER_SHORT            = 9;
    public static final int MARKER_DOUBLE           = 10;
    public static final int MARKER_DATE             = 11;
    public static final int MARKER_STRINGBUILDER    = 12;

    public static boolean isHandled( Object value ) {

        if ( value instanceof Boolean         ||
             value instanceof Byte            ||
             value instanceof String          ||
             value instanceof Character       ||
             value instanceof StringBuffer    ||
             value instanceof StringBuilder   ||
             value instanceof Short           ||
             value instanceof Long            ||
             value instanceof Double          ||
             value instanceof Float           ||
             value instanceof Date            ||
             value instanceof Integer ) {

            return true;

        }

        return false;
        
    }

    // **** encode methods ******************************************************

    public static byte[] encode( Object value ) throws Exception {

        if ( value instanceof Boolean )
            return encode( (Boolean)value );

        if ( value instanceof Integer ) {
            Integer o = (Integer)value;
            return encode( o.intValue() );
        }

        if ( value instanceof String )
            return encode( (String)value );

        if ( value instanceof Character )
            return encode( (Character)value );

        if ( value instanceof Byte )
            return encode( (Byte)value );

        if ( value instanceof StringBuffer )
            return encode( (StringBuffer)value );

        if ( value instanceof StringBuilder )
            return encode( (StringBuilder)value );

        if ( value instanceof Short )
            return encode( (Short)value );

        if ( value instanceof Long ) {
            Long o = (Long)value;
            return encode( o.longValue() );
        }

        if ( value instanceof Double ) {
            Double o = (Double)value;
            return encode( o.doubleValue() );
        }

        if ( value instanceof Float ) {
            Float o = (Float)value;
            return encode( o.floatValue() );
        }

		if (value instanceof Date) {
			Date o = (Date) value;
			return encode(o);
		}

        return null;
        
    }

    public static byte[] encode(Date value) {
		byte[] b = getBytes(value.getTime());
		b[0] = MARKER_DATE;
		return b;
	}

    public static byte[] encode( Boolean value ) {

        byte[] b = new byte[2];

        b[0] = MARKER_BOOLEAN;
        
        if ( value.booleanValue() ) {
            b[1] = 1;
        } else {
            b[1] = 0;
        }

        return b;
        
    }

    public static byte[] encode( int value ) {

        byte[] b = getBytes( value );
        b[0] = MARKER_INTEGER;

        return b;
        
    }

    public static byte[] encode( Character value ) {

        byte[] result = encode( value.charValue() );

        result[0] = MARKER_CHARACTER;
        
        return result;

    }
    
    public static byte[] encode( String value ) throws Exception {

        byte[] svalue = value.getBytes( "UTF-8" );

        byte[] result = new byte[ svalue.length + 1 ];

        result[0] = MARKER_STRING;
        
        System.arraycopy( svalue, 0, result, 1, svalue.length );

        return result;
        
    }

    public static byte[] encode( Byte value ) {

        byte[] b = new byte[2];

        b[0] = MARKER_BYTE;

        b[1] = value.byteValue();
        
        return b;
        
    }

    public static byte[] encode( StringBuffer value ) throws Exception {

        byte[] b = encode( value.toString() );
        b[0] = MARKER_STRINGBUFFER;
        
        return b;
        
    }

    public static byte[] encode( StringBuilder value ) throws Exception {

        byte[] b = encode( value.toString() );
        b[0] = MARKER_STRINGBUILDER;
        
        return b;
        
    }

    public static byte[] encode( Short value ) throws Exception {

        byte[] b = encode( (int)value.shortValue() );
        b[0] = MARKER_SHORT;
        
        return b;
        
    }

    public static byte[] encode( long value ) throws Exception {

        byte[] b = getBytes( value );
        b[0] = MARKER_LONG;

        return b;
        
    }

    public static byte[] encode( double value ) throws Exception {

        byte[] b = encode( (long)Double.doubleToLongBits( value ) );
        b[0] = MARKER_DOUBLE;
        
        return b;
        
    }

    public static byte[] encode( float value ) throws Exception {

        byte[] b = encode( (int)Float.floatToIntBits( value ) );
        b[0] = MARKER_FLOAT;
        
        return b;
        
    }

    public static byte[] getBytes( long value ) {

        byte b0 = (byte)((value >> 56) & 0xFF);
        byte b1 = (byte)((value >> 48) & 0xFF);
        byte b2 = (byte)((value >> 40) & 0xFF);
        byte b3 = (byte)((value >> 32) & 0xFF);
        byte b4 = (byte)((value >> 24) & 0xFF);
        byte b5 = (byte)((value >> 16) & 0xFF);
        byte b6 = (byte)((value >> 8) & 0xFF);
        byte b7 = (byte)((value >> 0) & 0xFF);

        byte[] b = new byte[9];
        b[1] = b0;
        b[2] = b1;
        b[3] = b2;
        b[4] = b3;
        b[5] = b4;
        b[6] = b5;
        b[7] = b6;
        b[8] = b7;

        return b;
        
    }

    public static byte[] getBytes( int value ) {

        byte b0 = (byte)((value >> 24) & 0xFF);
        byte b1 = (byte)((value >> 16) & 0xFF);
        byte b2 = (byte)((value >> 8) & 0xFF);
        byte b3 = (byte)((value >> 0) & 0xFF);

        byte[] b = new byte[5];
        b[1] = b0;
        b[2] = b1;
        b[3] = b2;
        b[4] = b3;

        return b;
        
    }

    // **** decode methods ******************************************************

    public static Object decode( byte[] b) throws Exception {

        //something strange is going on.
        if ( b.length < 1 )
            return null;

        //determine what type this is:

        if ( b[0] == MARKER_BOOLEAN )
            return decodeBoolean( b );

        if ( b[0] == MARKER_INTEGER )
            return decodeInteger( b );

        if ( b[0] == MARKER_STRING )
            return decodeString( b );

        if ( b[0] == MARKER_CHARACTER )
            return decodeCharacter( b );

        if ( b[0] == MARKER_BYTE )
            return decodeByte( b );

        if ( b[0] == MARKER_STRINGBUFFER )
            return decodeStringBuffer( b );

        if ( b[0] == MARKER_SHORT )
            return decodeShort( b );

        if ( b[0] == MARKER_LONG )
            return decodeLong( b );

        if ( b[0] == MARKER_DOUBLE )
            return decodeDouble( b );

        if ( b[0] == MARKER_FLOAT )
            return decodeFloat( b );

        if ( b[0] == MARKER_DATE )
            return decodeDate( b );

        return null;

    }

	public static Date decodeDate(byte[] b) {
		return new Date(toLong(b));
	}

    public static Boolean decodeBoolean( byte[] b ) {

        boolean value = b[1] == 1;

        if ( value )
            return Boolean.TRUE;

        return Boolean.FALSE;
        
    }

    public static Integer decodeInteger( byte[] b ) {

        return new Integer( toInt( b ) );
        
    }

    public static String decodeString( byte[] b ) throws Exception {
		return new String(b, 1, b.length - 1, "UTF-8");
    }

    public static Character decodeCharacter( byte[] b ) {

        return new Character( (char)decodeInteger( b ).intValue() );
        
    }

    public static Byte decodeByte( byte[] b ) {

        byte value = b[1];

        return new Byte( value );
        
    }

    public static StringBuffer decodeStringBuffer( byte[] b ) throws Exception {

        return new StringBuffer( decodeString( b ) );
        
    }

    public static StringBuilder decodeStringBuilder( byte[] b ) throws Exception {

        return new StringBuilder( decodeString( b ) );
        
    }

    public static Short decodeShort( byte[] b ) throws Exception {

        //FIXME: this generates an extra object we don't need
        return new Short( (short)decodeInteger( b ).intValue() );
        
    }

    public static Long decodeLong( byte[] b ) throws Exception {

        //FIXME: this generates an extra object we don't need
        return new Long( toLong( b ) );
        
    }

    public static Double decodeDouble( byte[] b ) throws Exception {

        //FIXME: this generates an extra object we don't need
        
        Long l = decodeLong( b );
        
        return new Double( Double.longBitsToDouble( l.longValue() ) );
        
    }

    public static Float decodeFloat( byte[] b ) throws Exception {

        //FIXME: this generates an extra object we don't need
        
        Integer l = decodeInteger( b );
        
        return new Float( Float.intBitsToFloat( l.intValue() ) );
        
    }

    public static int toInt( byte[] b ) {

        //This works by taking each of the bit patterns and converting them to
        //ints taking into account 2s complement and then adding them..
        
        return (((((int) b[4]) & 0xFF) << 32) +
                ((((int) b[3]) & 0xFF) << 40) +
                ((((int) b[2]) & 0xFF) << 48) +
                ((((int) b[1]) & 0xFF) << 56));
    }    

    public static long toLong( byte[] b ) {

        //FIXME: this is sad in that it takes up 16 bytes instead of JUST 8
        //bytes and wastes memory.  We could use a memcached flag to enable
        //special treatment for 64bit types
        
        //This works by taking each of the bit patterns and converting them to
        //ints taking into account 2s complement and then adding them..

        return ((((long) b[8]) & 0xFF) +
                ((((long) b[7]) & 0xFF) << 8) +
                ((((long) b[6]) & 0xFF) << 16) +
                ((((long) b[5]) & 0xFF) << 24) +
                ((((long) b[4]) & 0xFF) << 32) +
                ((((long) b[3]) & 0xFF) << 40) +
                ((((long) b[2]) & 0xFF) << 48) +
                ((((long) b[1]) & 0xFF) << 56));
    }    

	/*
    public static void main( String[] args ) throws Exception {

//         int value = -100;

//         byte[] b = getBytes( value );

//         int result = toInt( b );

//         System.out.println( " FIXME: (debug): result: " + result );
//         System.out.println( " FIXME: (debug): value: " + value );
        
//         if ( result != value )
//             throw new Exception();
        
    }
	*/
}
