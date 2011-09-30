/**
 * 
 */
package com.schooner.MemCached;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;

import junit.framework.TestCase;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;

/**
 * @author newrootwang
 * 
 */
public class WhalinScenarioTest extends TestCase {

	private String[] hosts;

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

	@Override
	protected void setUp() throws Exception {
		String servers = System.getProperty("memcached.host");
		hosts = servers.split(",");
		super.setUp();
	}

	public void testDefault() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		MemCachedClient mc = new MemCachedClient();
		mc.set("foo", "hello");
		assertEquals("hello", mc.get("foo"));
		mc.set("你好", "hello");
		assertEquals("hello", mc.get("你好"));
		mc.flushAll();
		pool.shutDown();
	}

	public void testNoSanitize() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		MemCachedClient mc = new MemCachedClient();
		mc.setSanitizeKeys(false);
		mc.set("你好", "hello");
		assertEquals("hello", mc.get("你好"));
		mc.flushAll();
		pool.shutDown();
	}

	public void testMaxConn() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.setInitConn(5);
		pool.setMaxConn(19);
		pool.initialize();
		assertEquals(19, pool.getMaxConn());
		final MemCachedClient mc = new MemCachedClient();
		Thread[] threads = new Thread[20];
		assertEquals(pool.getInitConn(), SchoonerSockIOPool.getInstance().socketPool.get(hosts[0]).getNumIdle());
		for (int i = 0; i < 20; ++i) {
			threads[i] = new Thread() {
				@Override
				public void run() {
					for (int i = 0; i < 200; ++i) {
						String key = "foo" + Thread.currentThread().getId() + i;
						mc.set(key, "hello");
						assertEquals("hello", mc.get(key));
					}
					super.run();
				}
			};
			threads[i].start();
		}
		for (int i = 0; i < 20; ++i) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		assertTrue(SchoonerSockIOPool.getInstance().socketPool.get(hosts[0]).getNumActive() < 20);
		mc.flushAll();
		pool.shutDown();
	}

	public void testMinConn() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.setMinConn(2);
		pool.initialize();
		assertEquals(2, pool.getMinConn());
		final MemCachedClient mc = new MemCachedClient();
		Thread thread = new Thread();
		assertEquals(2, SchoonerSockIOPool.getInstance().socketPool.get(hosts[0]).getMinIdle());
		thread = new Thread() {
			@Override
			public void run() {
				for (int i = 0; i < 200; ++i) {
					String key = "foo" + Thread.currentThread().getId() + i;
					mc.set(key, "hello");
					assertEquals("hello", mc.get(key));
				}
				super.run();
			}
		};
		thread.start();
		try {
			thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertEquals(2, SchoonerSockIOPool.getInstance().socketPool.get(hosts[0]).getNumActive());
		mc.flushAll();
		pool.shutDown();
	}

	public void testSetWeight() {
		SockIOPool pool = SockIOPool.getInstance();
		Integer[] weights = new Integer[] { new Integer(90), new Integer(10) };
		pool.setServers(hosts);
		pool.setWeights(weights);
		pool.initialize();
		MemCachedClient mc = new MemCachedClient();
		for (int i = 0; i < 100; i++)
			mc.set("key " + i, "value " + i);
		pool.shutDown();

		pool = SockIOPool.getInstance("check");
		pool.setServers(new String[] { hosts[0] });
		int count = 0;
		pool.initialize();
		mc = new MemCachedClient("check");
		for (int i = 0; i < 100; i++) {
			if (mc.get("key " + i) != null)
				count++;
		}
		assertTrue(count <= 95 && count >= 85);
		pool.shutDown();
	}

	public void testIsInitialize() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		assertEquals(false, pool.isInitialized());
		pool.initialize();
		assertEquals(true, pool.isInitialized());
		pool.shutDown();
	}

	public void testSetServers() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		assertEquals(hosts, pool.getServers());
		pool.shutDown();
	}

	public void testSetHashingAlg() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setHashingAlg(SockIOPool.NATIVE_HASH);
		pool.initialize();
		assertEquals(SockIOPool.NATIVE_HASH, pool.getHashingAlg());
		final MemCachedClient mc = new MemCachedClient();
		mc.set("key", "value");
		mc.flushAll();
		pool.shutDown();
	}

	public void testSetNagle() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setNagle(true);
		pool.initialize();
		assertEquals(true, pool.getNagle());
		final MemCachedClient mc = new MemCachedClient();
		mc.set("key", "value");
		mc.flushAll();
		pool.shutDown();
	}

	public void testMemCachedClientWithBoolean() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.set("key", "value");
		assertEquals("value", mc.get("key"));
		mc.flushAll();
		pool.shutDown();
	}

	public void testGetCounter() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.set("long", "1000");
		assertEquals("1000", mc.get("long"));
		assertEquals(1000, mc.getCounter("long"));
		mc.flushAll();
		pool.shutDown();
	}

	public void testStoreCounter() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.storeCounter("long", 1000);
		assertEquals(mc.get("long"), new Long(1000));
		mc.flushAll();
		pool.shutDown();
	}

	public void testSetDefaultEncoding() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.setDefaultEncoding("UTF-8");
		Class<?> classType = mc.getClass();
		Field[] fs = classType.getDeclaredFields();
		try {
			for (Field f : fs) {
				if (f.getName().equals("client")) {
					f.setAccessible(true);
					MemCachedClient innerMC = (MemCachedClient) f.get(mc);
					for (Field f2 : fs) {
						if (f2.getName().equals("defaultEncoding")) {
							f2.setAccessible(true);
							assertEquals(f2.get(innerMC), "UTF-8");
							break;
						}
					}
					break;
				}
			}
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		mc.flushAll();
		pool.shutDown();
	}

	public void testSetPrimitiveAsString() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.setPrimitiveAsString(true);
		Class<?> classType = mc.getClass();
		Field[] fs = classType.getDeclaredFields();
		try {
			for (Field f : fs) {
				if (f.getName().equals("client")) {
					f.setAccessible(true);
					MemCachedClient innerMC = (MemCachedClient) f.get(mc);
					for (Field f2 : fs) {
						if (f2.getName().equals("primitiveAsString")) {
							f2.setAccessible(true);
							assertEquals(f2.getBoolean(innerMC), true);
							break;
						}
					}
					break;
				}
			}
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		mc.flushAll();
		pool.shutDown();
	}

	public void testSetAliveCheck() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.setAliveCheck(true);
		assertEquals(true, pool.getAliveCheck());
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.set("key", "value");
		assertEquals("value", mc.get("key"));
		mc.flushAll();
		pool.shutDown();
	}

	public void testGetConnection() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.setMaxConn(10);
		pool.initialize();
		assertNotNull(pool.getConnection(hosts[0]));
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.set("key", "value");
		assertEquals("value", mc.get("key"));
		mc.flushAll();
		pool.shutDown();
	}

	public void testGetFailback() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.setFailback(true);
		pool.initialize();
		assertEquals(true, pool.getFailback());
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.set("key", "value");
		assertEquals("value", mc.get("key"));
		mc.flushAll();
		pool.shutDown();
	}

	public void testGetFailover() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.setFailover(true);
		pool.initialize();
		assertEquals(true, pool.getFailover());
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.set("key", "value");
		assertEquals("value", mc.get("key"));
		mc.flushAll();
		pool.shutDown();
	}

	public void testGetMultiArray() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		String[] keys = new String[10];
		String[] values = new String[10];
		for (int i = 0; i < 10; i++) {
			keys[i] = "key " + i;
			values[i] = "value " + i;
			mc.set(keys[i], values[i]);
		}
		Map<String, Object> rValues = mc.getMulti(keys, null, false);
		for (int i = 0; i < 10; i++) {
			assertEquals(values[i], rValues.get(keys[i]));
		}
		mc.flushAll();
		pool.shutDown();
	}

	public void testGetMutiArrayStringArrayIntegerArrayBool() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.set("foo1", "bar1", 1);
		mc.set("foo2", "bar2", 2);
		mc.set("foo3", "bar3", 3);
		String[] args = { "foo1", "foo2", "foo3" };
		String[] expected = { "bar1", "bar2", "bar3" };
		Integer[] hashcodes = { 1, 2, 3 };
		Object[] actual = mc.getMultiArray(args, hashcodes, true);
		assertEquals(expected.length, actual.length);
		for (int i = 0; i < actual.length; i++) {
			assertEquals(expected[i], actual[i]);
		}
		mc.flushAll();
		pool.shutDown();
	}

	public void testGetWithIOException() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		TestClass expect = new TestClass("bar1", "bar2", 3);
		mc.set("foo", expect);
		Object actual = mc.get("foo");
		assertEquals(expect, actual);
		mc.setTransCoder(new TransCoder() {

			public int encode(SockOutputStream out, Object object) throws IOException {
				throw new IOException();
			}

			public Object decode(InputStream input) throws IOException {
				throw new IOException();
			}
		});
		actual = mc.get("foo");
		assertNotSame(expect, actual);
		actual = mc.get("foo", null, false);
		assertNotSame(expect, actual);
		mc.flushAll();
		pool.shutDown();
	}

	public void testGetsWithIOException() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		TestClass expect = new TestClass("bar1", "bar2", 3);
		mc.set("foo", expect);
		Object actual = mc.get("foo");
		assertEquals(expect, actual);
		mc.setTransCoder(new TransCoder() {

			public int encode(SockOutputStream out, Object object) throws IOException {
				throw new IOException();
			}

			public Object decode(InputStream input) throws IOException {
				throw new IOException();
			}
		});
		actual = mc.gets("foo");
		assertNotSame(expect, actual);
		mc.flushAll();
		pool.shutDown();
	}

	public void testSetWithIOException() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		mc.setTransCoder(new TransCoder() {

			public int encode(SockOutputStream out, Object object) throws IOException {
				throw new IOException();
			}

			public Object decode(InputStream input) throws IOException {
				throw new IOException();
			}
		});
		TestClass expect = new TestClass("bar1", "bar2", 3);
		mc.set("foo", expect);
		Object actual = mc.get("foo");
		assertNotSame(expect, actual);
		mc.flushAll();
		pool.shutDown();
	}

	public void testSockIO() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		SockIOPool.SockIO sock = SockIOPool.getInstance().getSock(hosts[0]);
		try {
			sock.write(new String("version\r\n").getBytes());
			sock.flush();
			String version = sock.readLine();
			assertNotNull(version);
			System.out.println("Version: " + version);
		} catch (IOException ioe) {
			System.out.println("io exception thrown");
		}
		sock.close();
		pool.shutDown();
	}

	public void testGetMulti() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, false);
		String[] keys = new String[10];
		String[] values = new String[10];
		for (int i = 0; i < 10; i++) {
			keys[i] = "key/ " + i;
			values[i] = "value " + i;
			mc.set(keys[i], values[i]);
		}
		Map<String, Object> rValues = mc.getMulti(keys);
		for (int i = 0; i < 10; i++) {
			assertEquals(values[i], rValues.get("key/ " + i));
		}
		mc.flushAll();
		pool.shutDown();
	}

	public void testGetMultiBin() {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(hosts);
		pool.initialize();
		final MemCachedClient mc = new MemCachedClient(true, true);
		mc.setSanitizeKeys(true);
		String[] keys = new String[10];
		String[] values = new String[10];
		for (int i = 0; i < 10; i++) {
			keys[i] = "key/ " + i;
			values[i] = "value " + i;
			mc.set(keys[i], values[i]);
		}
		Map<String, Object> rValues = mc.getMulti(keys);
		for (int i = 0; i < 10; i++) {
			assertEquals(values[i], rValues.get("key/ " + i));
		}
		mc.flushAll();
		pool.shutDown();
	}
}
