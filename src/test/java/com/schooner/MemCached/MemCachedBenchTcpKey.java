package com.schooner.MemCached;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.whalin.MemCached.MemCachedClient;

public class MemCachedBenchTcpKey extends TestCase {
	private static final String FILE_URL = "spring-memcached-TCP.xml";

	private static final String BEAN_NAME = "memcachedClient";

	private ClassPathXmlApplicationContext context;

	public String getFileUrl() {
		return FILE_URL;
	}

	@Override
	public void setUp() {
		String fileUrl = this.getFileUrl();
		context = new ClassPathXmlApplicationContext(fileUrl);
		MemCachedClient memcachedClient = getClient();
		memcachedClient.flushAll();
	}

	public void testStringKey() {
		System.out.println("-----test string key begin-----");
		MemCachedClient memcachedClient = getClient();
		memcachedClient.set("test", "test");
		System.out.println(memcachedClient.get(new String("test")));
		System.out.println("-----test string key end-----");
	}

	public void testStringBuilderKey() {
		System.out.println("-----test stringbuilder key begin-----");
		MemCachedClient memcachedClient = getClient();
		StringBuilder builder = new StringBuilder("test");
		memcachedClient.set(builder, "test");
		builder = new StringBuilder("test");
		System.out.println(memcachedClient.get(builder));
		System.out.println("-----test stringbuilder key end-----");
	}

	public void testStringBufferKey() {
		System.out.println("-----test stringbuffer key begin-----");
		MemCachedClient memcachedClient = getClient();
		StringBuffer buffer = new StringBuffer("test");
		memcachedClient.set(buffer, "test");
		buffer = new StringBuffer("test");
		System.out.println(memcachedClient.get(buffer));
		System.out.println("-----test stringbuffer key end-----");
	}

	public void testBooleanKey() {
		System.out.println("-----test Boolean key begin-----");
		MemCachedClient memcachedClient = getClient();
		memcachedClient.set(true, "test");
		Boolean b = true;
		System.out.println(memcachedClient.get(b));
		System.out.println("-----test Boolean key end-----");
	}

	public void testByteKey() {
		System.out.println("-----test byte key begin-----");
		MemCachedClient memcachedClient = getClient();
		memcachedClient.set((byte) 1, "test");
		Byte b = 1;
		System.out.println(memcachedClient.get(b));
		System.out.println("-----test byte key end-----");
	}

	public void testShortKey() {
		System.out.println("-----test Short key begin-----");
		MemCachedClient memcachedClient = getClient();
		memcachedClient.set((short) 1, "test");
		Short s = 1;
		System.out.println(memcachedClient.get(s));
		System.out.println("-----test Short key end-----");
	}

	public void testCharacterKey() {
		System.out.println("-----test Character key begin-----");
		MemCachedClient memcachedClient = getClient();
		memcachedClient.set('c', "test");
		Character c = 'c';
		System.out.println(memcachedClient.get(c));
		System.out.println("-----test Character key end-----");
	}

	public void testIntegerKey() {
		System.out.println("-----test Integer key begin-----");
		MemCachedClient memcachedClient = getClient();
		memcachedClient.set(1, "test");
		Integer i = 1;
		System.out.println(memcachedClient.get(i));
		System.out.println("-----test Integer key end-----");
	}

	public void testLongKey() {
		System.out.println("-----test Long key begin-----");
		MemCachedClient memcachedClient = getClient();
		memcachedClient.set(1L, "test");
		Long lg = 1L;
		System.out.println(memcachedClient.get(lg));
		System.out.println("-----test Long key end-----");
	}

	public void testFloatKey() {
		System.out.println("-----test Float key begin-----");
		MemCachedClient memcachedClient = getClient();
		memcachedClient.set(1.0F, "test");
		Float f = 1.0F;
		System.out.println(memcachedClient.get(f));
		System.out.println("-----test Float key end-----");
	}

	public void testDoubleKey() {
		System.out.println("-----test Double key begin-----");
		MemCachedClient memcachedClient = getClient();
		memcachedClient.set(1.0, "test");
		Double d = 1.0;
		System.out.println(memcachedClient.get(d));
		System.out.println("-----test Double key end-----");
	}

	public void testDateKey() {
		System.out.println("-----test array key begin-----");
		MemCachedClient memcachedClient = getClient();
		Date date = Calendar.getInstance().getTime();
		memcachedClient.set(date, "test");
		System.out.println(memcachedClient.get(date));
		System.out.println("-----test array key end-----");
	}

	public void testArrayKey() {
		System.out.println("-----test array key begin-----");
		MemCachedClient memcachedClient = getClient();
		User[] array = new User[10];
		memcachedClient.set(array, "test");
		array = new User[10];
		System.out.println(memcachedClient.get(array));
		System.out.println("-----test array key end-----");
	}

	public void testSerializableKey() {
		System.out.println("-----test Serializable key begin-----");
		MemCachedClient memcachedClient = getClient();
		SUser user = new SUser("test");
		memcachedClient.set(user, "test");
		user = new SUser("test");
		System.out.println(memcachedClient.get(user));
		System.out.println("-----test Serializabl key end-----");
	}

	public void testExternalizableKey() {
		System.out.println("-----test Externalizable key begin-----");
		MemCachedClient memcachedClient = getClient();
		EUser user = new EUser("test");
		memcachedClient.set(user, "test");
		user = new EUser("test");
		System.out.println(memcachedClient.get(user));
		System.out.println("-----test Externalizable key end-----");
	}

	private MemCachedClient getClient() {
		MemCachedClient memcachedClient = (MemCachedClient) context
				.getBean(BEAN_NAME);
		return memcachedClient;
	}

	@Override
	public void tearDown() {
		MemCachedClient memcachedClient = getClient();
		showKeys(memcachedClient);
		memcachedClient.flushAll();
		if (context != null) {
			context.close();
		}
	}

	private static void showKeys(MemCachedClient memcachedClient) {
		System.out.println("----show keys begin-----");
		Map<String, Map<String, String>> map = memcachedClient.statsItems();
		Set<String> set = new HashSet<String>();
		for (Map<String, String> m : map.values()) {
			for (String s : m.keySet()) {
				int first = s.indexOf(":");
				int second = s.indexOf(":", first + 1);
				set.add(s.substring(first + 1, second));
			}
		}
		Set<String> keySet = new HashSet<String>();
		for (String s : set) {
			int slab = Integer.valueOf(s);
			Map<String, Map<String, String>> dumpMap = memcachedClient
					.statsCacheDump(slab, 0);
			for (Map<String, String> m : dumpMap.values()) {
				keySet.addAll(m.keySet());
			}
		}
		System.out.println(keySet);
		System.out.println("-----show keys end-----");
		System.out.println();
	}

	private static class User {
	}

	private static class SUser implements Serializable {
		private static final long serialVersionUID = -1921923046163600659L;

		private String name;

		SUser(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "hashcode:" + this.hashCode() + "	name:" + name;
		}
	}

	private static class EUser implements Externalizable {
		private static final long serialVersionUID = 8674506162535178907L;

		private String name;

		EUser(String name) {
			this.name = name;
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeObject(name);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			name = (String) in.readObject();
		}

		@Override
		public String toString() {
			return "hashcode:" + this.hashCode() + "	name:" + name;
		}
	}
}
