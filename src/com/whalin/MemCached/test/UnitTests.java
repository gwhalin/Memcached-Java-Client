/**
 * UnitTests.java
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
 * @author Kevin Burton
 * @author greg whalin <greg@meetup.com> 
 * @version 1.2
 */
package com.danga.MemCached.test;

import com.danga.MemCached.*;
import java.util.*;
import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

public class UnitTests {
	
	// logger
	private static Logger log =
		Logger.getLogger( UnitTests.class.getName() );

    public static MemCachedClient mc  = null;

    public static void test1() {
        mc.set( "foo", Boolean.TRUE );
        Boolean b = (Boolean)mc.get( "foo" );
		assert b.booleanValue();
		log.error( "+ store/retrieve Boolean type test passed" );
    }

    public static void test2() {
        mc.set( "foo", new Integer( Integer.MAX_VALUE ) );
        Integer i = (Integer)mc.get( "foo" );
        assert i.intValue() == Integer.MAX_VALUE;
		log.error( "+ store/retrieve Integer type test passed" );
    }

    public static void test3() {
        String input = "test of string encoding";
        mc.set( "foo", input );
        String s = (String)mc.get( "foo" );
		assert s.equals( input );
		log.error( "+ store/retrieve String type test passed" );
    }
    
    public static void test4() {
        mc.set( "foo", new Character( 'z' ) );
        Character c = (Character)mc.get( "foo" );
		assert c.charValue() == 'z';
		log.error( "+ store/retrieve Character type test passed" );
    }

    public static void test5() {
        mc.set( "foo", new Byte( (byte)127 ) );
        Byte b = (Byte)mc.get( "foo" );
		assert b.byteValue() == 127;
		log.error( "+ store/retrieve Byte type test passed" );
    }

    public static void test6() {
        mc.set( "foo", new StringBuffer( "hello" ) );
        StringBuffer o = (StringBuffer)mc.get( "foo" );
		assert o.toString().equals( "hello" );
		log.error( "+ store/retrieve StringBuffer type test passed" );
    }

    public static void test7() {
        mc.set( "foo", new Short( (short)100 ) );
        Short o = (Short)mc.get( "foo" );
		assert o.shortValue() == 100;
		log.error( "+ store/retrieve Short type test passed" );
    }

    public static void test8() {
        mc.set( "foo", new Long( Long.MAX_VALUE ) );
        Long o = (Long)mc.get( "foo" );
		assert o.longValue() == Long.MAX_VALUE;
		log.error( "+ store/retrieve Long type test passed" );
    }

    public static void test9() {
        mc.set( "foo", new Double( 1.1 ) );
        Double o = (Double)mc.get( "foo" );
		assert o.doubleValue() == 1.1;
		log.error( "+ store/retrieve Double type test passed" );
    }

    public static void test10() {
        mc.set( "foo", new Float( 1.1f ) );
        Float o = (Float)mc.get( "foo" );
		assert o.floatValue() == 1.1f;
		log.error( "+ store/retrieve Float type test passed" );
    }

    public static void test11() {
        mc.set( "foo", new Integer( 100 ), new Date( System.currentTimeMillis() ));
        try { Thread.sleep( 1000 ); } catch ( Exception ex ) { }
        assert mc.get( "foo" ) == null;
		log.error( "+ store/retrieve w/ expiration test passed" );
    }

	public static void test12() {
		long i = 0;
		mc.storeCounter("foo", i);
		mc.incr("foo"); // foo now == 1
		mc.incr("foo", (long)5); // foo now == 6
		long j = mc.decr("foo", (long)2); // foo now == 4
		assert j == 4;
		assert j == mc.getCounter( "foo" );
		log.error( "+ incr/decr test passed" );
	}

	public static void test13() {
		Date d1 = new Date();
		mc.set("foo", d1);
		Date d2 = (Date) mc.get("foo");
		assert d1.equals( d2 );
		log.error( "+ store/retrieve Date type test passed" );
	}

	public static void test14() {
		assert !mc.keyExists( "foobar123" );
		mc.set( "foobar123", new Integer( 100000) );
		assert mc.keyExists( "foobar123" );
		log.error( "+ store/retrieve test passed" );

		assert !mc.keyExists( "counterTest123" );
		mc.storeCounter( "counterTest123", 0 );
		assert mc.keyExists( "counterTest123" );
		log.error( "+ counter store test passed" );
	}

	public static void test15() {

		Map stats = mc.statsItems();
		assert stats != null;

		stats = mc.statsSlabs();
		assert stats != null;

		log.error( "+ stats test passed" );
	}

	public static void test16() {
        assert !mc.set( "foo", null );
		log.error( "+ invalid data store [null] test passed" );
	}
    
	public static void test17() {
        mc.set( "foo bar", Boolean.TRUE );
        Boolean b = (Boolean)mc.get( "foo bar" );
		assert b.booleanValue();
		log.error( "+ store/retrieve Boolean type test passed" );
	}
    
	public static void test18() {
		long i = 0;
		mc.addOrIncr( "foo" ); // foo now == 0
		mc.incr( "foo" ); // foo now == 1
		mc.incr( "foo", (long)5 ); // foo now == 6

		mc.addOrIncr( "foo" ); // foo now 7

		long j = mc.decr( "foo", (long)3 ); // foo now == 4
		assert j == 4;
		assert j == mc.getCounter( "foo" );

		log.error( "+ incr/decr test passed" );
	}

	public static void test19() {
		int max = 100;
		String[] keys = new String[ max ];
		for ( int i=0; i<max; i++ ) {
			keys[i] = Integer.toString(i);
			mc.set( keys[i], "value"+i );
		}
		
		Map<String,Object> results = mc.getMulti( keys );
		for ( int i=0; i<max; i++ ) {
			assert results.get( keys[i]).equals( "value"+i );
		}
		log.error( "+ getMulti test passed" );
	}
	
	public static void test20( int max, int skip, int start ) {
		log.warn( String.format( "test 20 starting with start=%5d skip=%5d max=%7d", start, skip, max ) );
		int numEntries = max/skip+1;
		String[] keys = new String[ numEntries ];
		byte[][] vals = new byte[ numEntries ][];
		
		int size = start;
		for ( int i=0; i<numEntries; i++ ) {
			keys[i] = Integer.toString( size );
			vals[i] = new byte[size + 1];
			for ( int j=0; j<size + 1; j++ )
				vals[i][j] = (byte)j;
			
			mc.set( keys[i], vals[i] );
			size += skip;
		}
		
		Map<String,Object> results = mc.getMulti( keys );
		for ( int i=0; i<numEntries; i++ )
			assert Arrays.equals( (byte[])results.get( keys[i]), vals[i] );
		
		log.warn( String.format( "test 20 finished with start=%5d skip=%5d max=%7d", start, skip, max ) );
	}

    public static void test21() {
        mc.set( "foo", new StringBuilder( "hello" ) );
        StringBuilder o = (StringBuilder)mc.get( "foo" );
		assert o.toString().equals( "hello" );
		log.error( "+ store/retrieve StringBuilder type test passed" );
    }

    public static void test22() {
		byte[] b = new byte[10];
		for ( int i = 0; i < 10; i++ )
			b[i] = (byte)i;

        mc.set( "foo", b );
		assert Arrays.equals( (byte[])mc.get( "foo" ), b );
		log.error( "+ store/retrieve byte[] type test passed" );
    }

    public static void test23() {
		TestClass tc = new TestClass( "foo", "bar", new Integer( 32 ) );
        mc.set( "foo", tc );
		assert tc.equals( (TestClass)mc.get( "foo" ) );
		log.error( "+ store/retrieve serialized object test passed" );
    }

	public static void test24() {

		String[] allKeys = { "key1", "key2", "key3", "key4", "key5", "key6", "key7" };
		String[] setKeys = { "key1", "key3", "key5", "key7" };

		for ( String key : setKeys ) {
			mc.set( key, key );
		}

		Map<String,Object> results = mc.getMulti( allKeys );

		assert allKeys.length == results.size();
		for ( String key : setKeys ) {
			String val = (String)results.get( key );
			assert key.equals( val );
		}

		log.error( "+ getMulti w/ keys that don't exist test passed" );
	}

	public static void runAlTests( MemCachedClient mc ) {
		test14();
		for ( int t = 0; t < 2; t++ ) {
			mc.setCompressEnable( ( t&1 ) == 1 );
			
			test1();
			test2();
			test3();
			test4();
			test5();
			test6();
			test7();
			test8();
			test9();
			test10();
			test11();
			test12();
			test13();
			test15();
			test16();
			test17();
			test21();
			test22();
			test23();
			test24();
			
			for ( int i = 0; i < 3; i++ )
				test19();
			
			test20( 8191, 1, 0 );
			test20( 8192, 1, 0 );
			test20( 8193, 1, 0 );
			
			test20( 16384, 100, 0 );
			test20( 17000, 128, 0 );
			
			test20( 128*1024, 1023, 0 );
			test20( 128*1024, 1023, 1 );
			test20( 128*1024, 1024, 0 );
			test20( 128*1024, 1024, 1 );
			
			test20( 128*1024, 1023, 0 );
			test20( 128*1024, 1023, 1 );
			test20( 128*1024, 1024, 0 );
			test20( 128*1024, 1024, 1 );
			
			test20( 900*1024, 32*1024, 0 );
			test20( 900*1024, 32*1024, 1 );
		}

	}

	/**
	 * This runs through some simple tests of the MemCacheClient.
	 *
	 * Command line args:
	 * args[0] = number of threads to spawn
	 * args[1] = number of runs per thread
	 * args[2] = size of object to store 
	 *
	 * @param args the command line arguments
	 */
	public static void main(String[] args) {

		BasicConfigurator.configure();
		org.apache.log4j.Logger.getRootLogger().setLevel( Level.WARN );

		if ( !UnitTests.class.desiredAssertionStatus() ) {
			System.err.println( "WARNING: assertions are disabled!" );
			try { Thread.sleep( 3000 ); } catch ( InterruptedException e ) {}
		}
		
		String[] serverlist = {
			"192.168.1.50:1620",
			"192.168.1.50:1621",
			"192.168.1.50:1622",
			"192.168.1.50:1623",
			"192.168.1.50:1624",
			"192.168.1.50:1625",
			"192.168.1.50:1626",
			"192.168.1.50:1627",
			"192.168.1.50:1628",
			"192.168.1.50:1629"
		};

		Integer[] weights = { 1, 1, 1, 1, 10, 5, 1, 1, 1, 3 };

		if ( args.length > 0 )
			serverlist = args;

		// initialize the pool for memcache servers
		SockIOPool pool = SockIOPool.getInstance( "test" );
		pool.setServers( serverlist );
		pool.setWeights( weights );
		pool.setMaxConn( 250 );
		pool.setNagle( false );
		pool.setHashingAlg( SockIOPool.CONSISTENT_HASH );
		pool.initialize();

        mc = new MemCachedClient( "test" );
		runAlTests( mc );
	}

	/** 
	 * Class for testing serializing of objects. 
	 * 
	 * @author $Author: $
	 * @version $Revision: $ $Date: $
	 */
	public static final class TestClass implements Serializable {

		private String field1;
		private String field2;
		private Integer field3;

		public TestClass( String field1, String field2, Integer field3 ) {
			this.field1 = field1;
			this.field2 = field2;
			this.field3 = field3;
		}

		public String getField1() { return this.field1; }
		public String getField2() { return this.field2; }
		public Integer getField3() { return this.field3; }

		public boolean equals( Object o ) {
			if ( this == o ) return true;
			if ( !( o instanceof TestClass ) ) return false;

			TestClass obj = (TestClass)o;

			return ( ( this.field1 == obj.getField1() || ( this.field1 != null && this.field1.equals( obj.getField1() ) ) )
					&& ( this.field2 == obj.getField2() || ( this.field2 != null && this.field2.equals( obj.getField2() ) ) )
					&& ( this.field3 == obj.getField3() || ( this.field3 != null && this.field3.equals( obj.getField3() ) ) ) );
		}
	}
}
