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
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;

public class MemCachedClientAsciiTest extends TestCase {

	protected static MemCachedClient mc = null;
	private static String[] serverlist;
	private static final String characterSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

	private static synchronized String initString(int len) {
		StringBuffer sb = new StringBuffer(len);
		Random random = new Random();
		int size = characterSet.length();
		for (int i = 0; i < len; ++i) {
			sb.append(characterSet.charAt(random.nextInt(size)));
		}
		return sb.toString();
	}

	static {
		String servers = System.getProperty("memcached.host");
		serverlist = servers.split(",");

		// initialize the pool for memcache servers
		SockIOPool pool = SockIOPool.getInstance("test");
		pool.setBufferSize(3 * 1024 * 1024);
		pool.setServers(serverlist);
		pool.initialize();
	}

	protected void setUp() throws Exception {
		super.setUp();
		mc = new MemCachedClient("test");
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		assertNotNull(mc);
		mc.flushAll();
	}

	public void testFlushAll() {
		mc.set("foo1", "bar1");
		mc.set("foo2", "bar2");
		mc.flushAll();
		assertFalse(mc.keyExists("foo1"));
		assertFalse(mc.keyExists("foo2"));
	}

	public void testIsUseBinaryProtocol() {
		assertFalse(mc.isUseBinaryProtocol());
	}

	public void testKeyExsits() {
		boolean expected, actual;
		mc.set("foo", "bar");
		actual = mc.keyExists("foo");
		expected = true;
		assertEquals(expected, actual);
	}

	public void testSetBoolean() {
		assertTrue(mc.set("foo", Boolean.TRUE));
		Boolean b = (Boolean) mc.get("foo");
		assertEquals(b.booleanValue(), true);
	}

	public void testDelete() {
		assertFalse(mc.delete(null));
		mc.set("foo", Boolean.TRUE);
		Boolean b = (Boolean) mc.get("foo");
		assertEquals(b.booleanValue(), true);
		assertTrue(mc.delete("foo"));
		assertEquals(null, mc.get("foo"));
	}

	/**
	 * this test case will fail in memcached 1.4+.<br>
	 * memcached 1.4+ didn't support delete with expire time.
	 */
	public void testDeleteStringDate() {
		mc.set("foo", "bar");
		mc.delete("foo", new Date(1000));
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		boolean expected = mc.keyExists("foo");
		assertFalse(expected);
	}

	/**
	 * this test case will fail in memcached 1.4+.<br>
	 * memcached 1.4+ didn't support delete with expire time.
	 */
	public void testDeleteStringIntegerDate() {
		mc.set("foo", "bar", 10);
		mc.delete("foo", 10, new Date(1000));
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertNull(mc.get("foo", 10));
	}

	public void testSetInteger() {
		mc.set("foo", new Integer(Integer.MAX_VALUE));
		Integer i = (Integer) mc.get("foo");
		assertEquals(i.intValue(), Integer.MAX_VALUE);
	}

	public void testSetString() {
		String input = "test of string encoding";
		mc.set("foo", input);
		String s = (String) mc.get("foo");
		assertEquals(s, input);
	}

	public void testSetChar() {
		mc.set("foo", new Character('z'));
		Character c = (Character) mc.get("foo");
		assertEquals(c.charValue(), 'z');
	}

	public void testSetByte() {
		mc.set("foo", new Byte((byte) 127));
		Byte b = (Byte) mc.get("foo");
		assertEquals((byte) 127, b.byteValue());
	}

	public void testSetStringBuffer() {
		mc.set("foo", new StringBuffer("hello"));
		StringBuffer o = (StringBuffer) mc.get("foo");
		assertEquals(o.toString(), "hello");
	}

	public void testSetShort() {
		mc.set("foo", new Short((short) 100));
		Short o = (Short) mc.get("foo");
		assertEquals(o.shortValue(), (short) 100);
	}

	public void testSetLong() {
		mc.set("foo", new Long(Long.MAX_VALUE));
		Long o = (Long) mc.get("foo");
		assertEquals(o.longValue(), Long.MAX_VALUE);
	}

	public void testSetDouble() {
		mc.set("foo", new Double(1.1));
		Double o = (Double) mc.get("foo");
		assertEquals(o.doubleValue(), 1.1);
	}

	public void testSetFloat() {
		mc.set("foo", new Float(1.1f));
		Float o = (Float) mc.get("foo");
		assertEquals(o.floatValue(), 1.1f);
	}

	public void testSetExp() {
		mc.set("foo", new Integer(100), new Date(1000));
		try {
			Thread.sleep(2000);
		} catch (Exception ex) {
		}
		assertNull(mc.get("foo"));
	}

	public void testIncr() {
		long i = 0;
		long j = mc.addOrIncr("foo", i); // now == 0
		assertEquals(mc.get("foo"), new Long(i).toString());
		j = mc.incr("foo"); // foo now == 1
		j = mc.incr("foo", (long) 5); // foo now == 6
		j = mc.decr("foo", (long) 2); // foo now == 4
		assertEquals(4, j);
		j = mc.incr("foo1");
		assertEquals(-1, j);
	}

	public void testIncrStringLongInteger() {
		long expected, actual;
		mc.addOrIncr("foo", 1, 10);
		actual = mc.incr("foo", 5, 10);
		expected = 6;
		assertEquals(expected, actual);
	}

	public void testDecrString() {
		long expected, actual;
		actual = mc.addOrIncr("foo");
		mc.incr("foo", 5);

		expected = 4;
		actual = mc.decr("foo");
		assertEquals(expected, actual);
	}

	public void testDecrStringLongInteger() {
		long expected, actual;
		actual = mc.addOrIncr("foo", 1, 10);
		mc.incr("foo", 5, 10);

		expected = 3;
		actual = mc.decr("foo", 3, 10);
		assertEquals(expected, actual);
	}

	public void testSetDate() {
		Date d1 = new Date();
		mc.set("foo", d1);
		Date d2 = (Date) mc.get("foo");
		assertEquals(d1, d2);
	}

	public void testAddOrIncr() {

		assertEquals(mc.incr(null), -1);
		long j;
		j = mc.addOrIncr("foo"); // foo now == 0
		assertEquals(0, j);
		j = mc.incr("foo"); // foo now == 1
		j = mc.incr("foo", (long) 5); // foo now == 6

		j = mc.addOrIncr("foo", 1); // foo now 7

		j = mc.decr("foo", (long) 3); // foo now == 4
		assertEquals(4, j);
	}

	public void testAddOrDecrString() {

		long expected, actual;
		actual = mc.addOrDecr("foo");
		expected = 0;
		assertEquals(expected, actual);

		expected = 5;
		actual = mc.addOrIncr("foo", 5);
		assertEquals(expected, actual);

		actual = mc.addOrDecr("foo", 1);
		expected = 4;
		assertEquals(expected, actual);
	}

	public void testAddOrDecrStringLong() {

		long expected, actual;
		actual = mc.addOrDecr("foo", 2);
		expected = 2;
		assertEquals(expected, actual);

		expected = 7;
		actual = mc.addOrIncr("foo", 5);
		assertEquals(expected, actual);

		actual = mc.addOrDecr("foo", 3);
		expected = 4;
		assertEquals(expected, actual);
	}

	public void testAddOrDecrStringLongInteger() {

		long expected, actual;
		int hashcode = 10;
		actual = mc.addOrDecr("foo", 2, hashcode);
		expected = 2;
		assertEquals(expected, actual);

		expected = 7;
		actual = mc.addOrIncr("foo", 5, hashcode);
		assertEquals(expected, actual, hashcode);

		actual = mc.addOrDecr("foo", 3, hashcode);
		expected = 4;
		assertEquals(expected, actual);
	}

	public void testGetMulti() {
		int max = 100;
		String[] keys = new String[max];
		for (int i = 0; i < max; i++) {
			keys[i] = Integer.toString(i);
			mc.set(keys[i], "value" + i);
		}

		Map<String, Object> results = mc.getMulti(keys);
		for (int i = 0; i < max; i++) {
			assertEquals(results.get(keys[i]), "value" + i);
		}
	}

	public void testGetMutiArrayStringArray() {
		mc.set("foo1", "bar1");
		mc.set("foo2", "bar2");
		mc.set("foo3", "bar3");
		String[] args = { "foo1", "foo2", "foo3" };
		String[] expected = { "bar1", "bar2", "bar3" };
		Object[] actual = mc.getMultiArray(args);
		assertEquals(expected.length, actual.length);
		for (int i = 0; i < actual.length; i++) {
			assertEquals(expected[i], actual[i]);
		}
	}

	public void testGetMutiArrayStringArrayIntegerArray() {
		mc.set("foo1", "bar1", 1);
		mc.set("foo2", "bar2", 2);
		mc.set("foo3", "bar3", 3);
		String[] args = { "foo1", "foo2", "foo3" };
		String[] expected = { "bar1", "bar2", "bar3" };
		Integer[] hashcodes = { 1, 2, 3 };
		Object[] actual = mc.getMultiArray(args, hashcodes);
		assertEquals(expected.length, actual.length);
		for (int i = 0; i < actual.length; i++) {
			assertEquals(expected[i], actual[i]);
		}
	}

	public void testSetByteArray() {
		byte[] b = new byte[10];
		for (int i = 0; i < 10; i++)
			b[i] = (byte) i;

		mc.set("foo", b);
		assertTrue(Arrays.equals((byte[]) mc.get("foo"), b));
	}

	public void testSetObj() {
		TestClass tc = new TestClass("foo", "bar", new Integer(32));
		mc.set("foo", tc);
		TestClass tt = (TestClass) mc.get("foo");
		assertEquals(tc, tt);
	}

	public void testCusTransCoder() {
		TransCoder coder = new ObjectTransCoder() {
			@Override
			public void encode(OutputStream out, Object object) throws IOException {
				ByteArrayOutputStream bOut = new ByteArrayOutputStream();
				ObjectOutputStream oOut = new ObjectOutputStream(bOut);
				oOut.writeObject(object);
				byte[] bytes = bOut.toByteArray();
				for (byte b : bytes)
					out.write(b);
			}
		};
		mc.setTransCoder(coder);
		TestClass tc = new TestClass("foo", "bar", new Integer(32));
		mc.set("foo", tc);
		TestClass tt = (TestClass) mc.get("foo");
		assertEquals(tc, tt);
		tc = new TestClass("foo", initString(1024 * 2), new Integer(32));
		mc.set("foo", tc);
		tt = (TestClass) mc.get("foo");
		assertEquals(tc, tt);
	}

	public void testSetStringObjectDateInteger() {
		String expected, actual;
		mc.set("foo", "bar", new Date(1000), 10);
		expected = "bar";
		actual = (String) mc.get("foo", 10);
		assertEquals(expected, actual);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		actual = (String) mc.get("foo", 10);
		assertNull(actual);
	}

	public void testSetStringObjectInteger() {
		String expected, actual;
		mc.set("foo", "bar", 10);
		expected = "bar";
		actual = (String) mc.get("foo", 10);
		assertEquals(expected, actual);
	}

	public void testMultiKey() {

		String[] allKeys = { "key1", "key2", "key3", "key4", "key5", "key6", "key7" };
		String[] setKeys = { "key1", "key3", "key5", "key7" };

		for (String key : setKeys) {
			mc.set(key, key);
		}

		Map<String, Object> results = mc.getMulti(allKeys);

		assert allKeys.length == results.size();
		for (String key : setKeys) {
			String val = (String) results.get(key);
			assertEquals(key, val);
		}
	}

	public void testAdd() {
		mc.delete("foo");
		assertEquals(null, mc.get("foo"));
		mc.set("foo", "bar");
		String tt = (String) mc.get("foo");
		assertEquals("bar", tt);
		mc.add("foo", "bar2");
		String tt2 = (String) mc.get("foo");
		assertEquals("bar", tt2);
		assertFalse(mc.add(null, "bar"));
		assertFalse(mc.add("foo", null));
	}

	public void testAddStringObjectDate() {
		String expected, actual;
		Date expiry = new Date(1000);
		mc.add("foo", "bar", expiry);
		actual = (String) mc.get("foo");
		expected = "bar";
		assertEquals(expected, actual);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertFalse(mc.keyExists("foo"));
	}

	public void testAddStringObjectDateInteger() {
		String expected, actual;
		Date expiry = new Date(1000);
		mc.add("foo", "bar", expiry, 10);
		actual = (String) mc.get("foo", 10);
		expected = "bar";
		assertEquals(expected, actual);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		actual = (String) mc.get("foo", 10);
		assertNull(actual);
	}

	public void testReplaceStringObject() {
		String expected, actual;
		mc.set("foo", "bar1");
		mc.replace("foo", "bar2");
		expected = "bar2";
		actual = (String) mc.get("foo");
		assertEquals(expected, actual);
	}

	public void testReplaceStringObjectDate() {
		String expected, actual;
		mc.set("foo", "bar1");
		mc.replace("foo", "bar2", new Date(1000));
		expected = "bar2";
		actual = (String) mc.get("foo");
		assertEquals(expected, actual);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertFalse(mc.keyExists("foo"));
	}

	public void testReplaceStringObjectDateInteger() {
		String expected, actual;
		mc.set("foo", "bar1", 10);
		mc.replace("foo", "bar2", new Date(1000), 10);
		expected = "bar2";
		actual = (String) mc.get("foo", 10);
		assertEquals(expected, actual);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertNull(mc.get("foo", 10));
	}

	public void testReplaceStringObjectInteger() {
		String expected, actual;
		mc.set("foo", "bar1", 10);
		mc.replace("foo", "bar2", 10);
		expected = "bar2";
		actual = (String) mc.get("foo", 10);
		assertEquals(expected, actual);
	}

	public void testAppend() {
		String value = "aa";
		mc.append("aa", value);
		assertEquals(mc.get("aa"), null);
		mc.add("aa", value);
		assertEquals(mc.get("aa"), value);
		assertEquals(mc.append("aa", "bb"), true);
		assertEquals(mc.get("aa"), value + "bb");
	}

	public void testAppendStringObjectInteger() {
		String actual, expected;
		mc.add("foo", "abc", 10);
		actual = (String) mc.get("foo", 10);
		expected = "abc";
		assertEquals(expected, actual);

		mc.append("foo", "def", 10);
		actual = (String) mc.get("foo", 10);
		expected = "abcdef";
		assertEquals(expected, actual);
	}

	public void testPrepend() {
		String value = "aa";
		mc.prepend("aa", value);
		assertEquals(mc.get("aa"), null);
		mc.add("aa", value);
		assertEquals(mc.get("aa"), value);
		assertEquals(mc.prepend("aa", "bb"), true);
		assertEquals(mc.get("aa"), "bb" + value);
	}

	public void testPrependStringObjectInteger() {
		String expected, actual;
		mc.set("foo", "def", 10);
		mc.prepend("foo", "abc", 10);
		expected = "abcdef";
		actual = (String) mc.get("foo", 10);
		assertEquals(expected, actual);
	}

	public void testCas() {
		String value = "aa";
		mc.set("aa", value);
		MemcachedItem item = mc.gets("aa");
		assertEquals(value, item.getValue());
		mc.cas("aa", "bb", item.getCasUnique());
		item = mc.gets("aa");
		assertEquals("bb", item.getValue());
		mc.set("aa", "cc");
		assertEquals("cc", mc.get("aa"));
		mc.cas("aa", "dd", item.getCasUnique());
		assertEquals("cc", mc.get("aa"));
	}

	public void testCasStringObjectIntegerLong() {
		String expected, actual;
		mc.set("foo", "bar", 10);
		MemcachedItem item = mc.gets("foo", 10);
		expected = "bar";
		actual = (String) item.getValue();
		assertEquals(expected, actual);

		mc.cas("foo", "bar1", 10, item.getCasUnique());
		expected = "bar1";
		actual = (String) mc.get("foo", 10);
		assertEquals(expected, actual);

		mc.set("foo", "bar2", 10);
		expected = "bar2";
		actual = (String) mc.get("foo", 10);
		assertEquals(expected, actual);

		boolean res = mc.cas("foo", "bar3", 10, item.getCasUnique());
		assertFalse(res);
	}

	public void testCasStringObjectDateLong() {
		String expected, actual;
		mc.set("foo", "bar");
		MemcachedItem item = mc.gets("foo");
		expected = "bar";
		actual = (String) item.getValue();
		assertEquals(expected, actual);

		Date expiry = new Date(1000);
		mc.cas("foo", "bar1", expiry, item.getCasUnique());
		expected = "bar1";
		actual = (String) mc.get("foo");
		assertEquals(expected, actual);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertNull(mc.get("foo"));

		mc.set("foo", "bar2");
		expected = "bar2";
		actual = (String) mc.get("foo");
		assertEquals(expected, actual);

		boolean res = mc.cas("foo", "bar3", expiry, item.getCasUnique());
		assertFalse(res);
	}

	public void testCasStringObjectDateIntegerLong() {
		String expected, actual;
		mc.set("foo", "bar", 10);
		MemcachedItem item = mc.gets("foo", 10);
		expected = "bar";
		actual = (String) item.getValue();
		assertEquals(expected, actual);

		Date expiry = new Date(1000);
		mc.cas("foo", "bar1", expiry, 10, item.getCasUnique());
		expected = "bar1";
		actual = (String) mc.get("foo", 10);
		assertEquals(expected, actual);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		actual = (String) mc.get("foo", 10);
		assertNull(actual);

		mc.set("foo", "bar2", 10);
		expected = "bar2";
		actual = (String) mc.get("foo", 10);
		assertEquals(expected, actual);

		boolean res = mc.cas("foo", "bar3", expiry, 10, item.getCasUnique());
		assertFalse(res);
	}

	public void testBigData() {
		TestClass cls = new TestClass(initString(1024), initString(10240), 10240);
		for (int i = 0; i < 10; ++i) {
			mc.set("foo" + i, cls);
			assertEquals(cls, mc.get("foo" + i));
		}
		String buf = initString(10240);
		for (int i = 0; i < 10; ++i) {
			boolean res = mc.set("foo" + i, buf);
			assertEquals(true, res);
			assertEquals(buf, mc.get("foo" + i));
		}
	}

	public void testExtremeBigData() {
		TestClass cls = new TestClass(initString(1024), initString(10240), 10240);
		for (int i = 0; i < 10; ++i) {
			mc.set("foo" + i, cls);
			assertEquals(cls, mc.get("foo" + i));
		}
		String buf = initString(1024000 * 2);
		mc.set("foo", buf);
		assertNotNull(mc.get("foo"));
	}

	public void testStats() {
		Map<String, Map<String, String>> res = mc.stats();
		assertFalse(res.isEmpty());
	}

	public void testStatsStringArray() {
		Map<String, Map<String, String>> res = mc.stats(serverlist);
		assertFalse(res.isEmpty());
	}

	/**
	 * this test case will fail in memcached 1.4+.<br>
	 * memcached 1.4+ didn't support "stats items".
	 */
	public void testStatsItems() {
		Map<String, Map<String, String>> res = mc.statsItems();
		assertFalse(res.isEmpty());
	}

	/**
	 * this test case will fail in memcached 1.4+.<br>
	 * memcached 1.4+ didn't support "stats items".
	 */
	public void testStatsItemsStringArray() {
		Map<String, Map<String, String>> res = mc.statsItems(serverlist);
		assertFalse(res.isEmpty());
	}

	public void testStatsSlabs() {
		Map<String, Map<String, String>> res = mc.statsSlabs();
		assertFalse(res.isEmpty());
	}

	public void testStatsSlabsStringArray() {
		Map<String, Map<String, String>> res = mc.statsSlabs(serverlist);
		assertFalse(res.isEmpty());
	}

	/**
	 * this test case will fail in memcached 1.4+.<br>
	 * memcached 1.4+ didn't support "stats cachedump".
	 */
	public void testStatsCacheDumpStringArrayIntegerInteger() {
		Map<String, Map<String, String>> res = mc.statsCacheDump(serverlist, 1, 2);
		assertFalse(res.isEmpty());
	}

	/**
	 * this test case will fail in memcached 1.4+.<br>
	 * memcached 1.4+ didn't support "stats cachedump".
	 */
	public void testStatsCacheDumpIntegerInteger() {
		Map<String, Map<String, String>> res = mc.statsCacheDump(1, 2);
		assertFalse(res.isEmpty());
	}

	public void testNewCompactHash() {
		String servers = System.getProperty("memcached.host");
		serverlist = servers.split(",");

		// initialize the pool for memcache servers
		SchoonerSockIOPool pool = SchoonerSockIOPool.getInstance();
		pool.setServers(serverlist);
		pool.setNagle(false);
		pool.setHashingAlg(SchoonerSockIOPool.NEW_COMPAT_HASH);
		pool.initialize();
		MemCachedClient mc = new MemCachedClient();
		mc.set("foo", "bar");
		Object actual = mc.get("foo");
		assertEquals("bar", actual);
		pool.shutDown();
	}

	public void testOldCompatHash() {
		String servers = System.getProperty("memcached.host");
		serverlist = servers.split(",");

		// initialize the pool for memcache servers
		SchoonerSockIOPool pool = SchoonerSockIOPool.getInstance();
		pool.setServers(serverlist);
		pool.setNagle(false);
		pool.setHashingAlg(SchoonerSockIOPool.OLD_COMPAT_HASH);
		pool.initialize();
		MemCachedClient mc = new MemCachedClient();
		mc.set("foo", "bar");
		Object actual = mc.get("foo");
		assertEquals("bar", actual);
		pool.shutDown();
	}

	// TODO:
	public void testSyncString() {
		assertTrue(mc.sync("key"));
	}

	public void testSyncStringInteger() {
		assertFalse(mc.sync(null, 10));
		assertTrue(mc.sync("key", 10));
	}

	public void testSyncAll() {
		assertTrue(mc.syncAll());
	}

	public void testSyncAllStringArray() {
		assertTrue(mc.syncAll(serverlist));
	}

	public static final class TestClass implements Serializable {

		private static final long serialVersionUID = -6676639726514578903L;
		private String field1;
		private String field2;
		private Integer field3;

		public TestClass(String field1, String field2, Integer field3) {
			this.field1 = field1;
			this.field2 = field2;
			this.field3 = field3;
		}

		public String getField1() {
			return this.field1;
		}

		public String getField2() {
			return this.field2;
		}

		public Integer getField3() {
			return this.field3;
		}

		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (!(o instanceof TestClass))
				return false;

			TestClass obj = (TestClass) o;

			return ((this.field1 == obj.getField1() || (this.field1 != null && this.field1.equals(obj.getField1())))
					&& (this.field2 == obj.getField2() || (this.field2 != null && this.field2.equals(obj.getField2()))) && (this.field3 == obj
					.getField3() || (this.field3 != null && this.field3.equals(obj.getField3()))));
		}
	}

}
