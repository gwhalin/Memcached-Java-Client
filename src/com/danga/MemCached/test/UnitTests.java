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
    }

    public static void test2() {
        mc.set( "foo", new Integer( Integer.MAX_VALUE ) );
        Integer i = (Integer)mc.get( "foo" );
        assert i.intValue() == Integer.MAX_VALUE;
    }

    public static void test3() {
        String input = "test of string encoding";
        mc.set( "foo", input );
        String s = (String)mc.get( "foo" );
		assert s.equals( input );
    }
    
    public static void test4() {
        mc.set( "foo", new Character( 'z' ) );
        Character c = (Character)mc.get( "foo" );
		assert c.charValue() == 'z';
    }

    public static void test5() {
        mc.set( "foo", new Byte( (byte)127 ) );
        Byte b = (Byte)mc.get( "foo" );
		assert b.byteValue() == 127;
    }

    public static void test6() {
        mc.set( "foo", new StringBuffer( "hello" ) );
        StringBuffer o = (StringBuffer)mc.get( "foo" );
		assert o.toString().equals( "hello" );
    }

    public static void test7() {
        mc.set( "foo", new Short( (short)100 ) );
        Short o = (Short)mc.get( "foo" );
		assert o.shortValue() == 100;
    }

    public static void test8() {
        mc.set( "foo", new Long( Long.MAX_VALUE ) );
        Long o = (Long)mc.get( "foo" );
		assert o.longValue() == Long.MAX_VALUE;
    }

    public static void test9() {
        mc.set( "foo", new Double( 1.1 ) );
        Double o = (Double)mc.get( "foo" );
		assert o.doubleValue() == 1.1;
    }

    public static void test10() {
        mc.set( "foo", new Float( 1.1f ) );
        Float o = (Float)mc.get( "foo" );
		assert o.floatValue() == 1.1f;
    }

    public static void test11() {
        mc.set( "foo", new Integer( 100 ), new Date( System.currentTimeMillis() ));
        try { Thread.sleep( 1000 ); } catch ( Exception ex ) { }
        assert mc.get( "foo" ) == null;
    }

	public static void test12() {
		long i = 0;
		mc.storeCounter("foo", i);
		mc.incr("foo"); // foo now == 1
		mc.incr("foo", (long)5); // foo now == 6
		long j = mc.decr("foo", (long)2); // foo now == 4
		assert j == 4;
		assert j == mc.getCounter( "foo" );
	}

	public static void test13() {
		Date d1 = new Date();
		mc.set("foo", d1);
		Date d2 = (Date) mc.get("foo");
		assert d1.equals( d2 );
	}

	public static void test14() {
		assert !mc.keyExists( "foobar123" );
		mc.set( "foobar123", new Integer( 100000) );
		assert mc.keyExists( "foobar123" );

		assert !mc.keyExists( "counterTest123" );
		mc.storeCounter( "counterTest123", 0 );
		assert mc.keyExists( "counterTest123" );
	}

	public static void test15() {

		Map stats = mc.statsItems();
		assert stats != null;

		stats = mc.statsSlabs();
		assert stats != null;
	}

	public static void test16() {
        assert !mc.set( "foo", null );
	}
    
	public static void test17() {
        mc.set( "foo bar", Boolean.TRUE );
        Boolean b = (Boolean)mc.get( "foo bar" );
		assert b.booleanValue();
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

		String[] serverlist = { "192.168.1.1:1624"  };

		// initialize the pool for memcache servers
		SockIOPool pool = SockIOPool.getInstance( "test" );
		pool.setServers( serverlist );
		pool.setInitConn( 1 ); 
		pool.setMinConn( 3 );
		pool.setMaxConn( 250 );
		pool.setMaintSleep( 30 );
		pool.setNagle( false );
		pool.setSocketTO( 3000 );
		pool.initialize();

        mc = new MemCachedClient();
		mc.setPoolName( "test" );
        mc.setCompressEnable( false );

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
		test14();
		test15();
		test16();
		test17();
	}
}
