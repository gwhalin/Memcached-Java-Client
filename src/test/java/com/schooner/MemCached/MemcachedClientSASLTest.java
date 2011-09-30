package com.schooner.MemCached;

import java.util.Map;

import com.danga.MemCached.MemCachedClient;

public class MemcachedClientSASLTest {

	public static void main(String[] args) {
		int runs = Integer.parseInt(args[1]);
		int start = Integer.parseInt(args[2]);

		String servers = System.getProperty("memcached.host");
		String[] serverlist = servers.split(",");

		// initialize the pool for memcache servers
		SchoonerSockIOPool pool = SchoonerSockIOPool.getInstance("test", AuthInfo.typical("cacheuser", "123456"));
		pool.setServers(serverlist);

		pool.setInitConn(1);
		pool.setMaxConn(1);
		pool.setNagle(false);
		pool.setFailback(true);
		pool.setFailover(true);
		pool.initialize();

		// get client instance
		MemCachedClient mc;
		if (args[0].equals("ascii"))
			mc = new MemCachedClient("test", true, false);
		else
			mc = new MemCachedClient("test", true, true);

		Map<String, Map<String, String>> st = mc.stats(serverlist);
		for (String k : st.keySet()) {
			st.get(k).get("auth");
		}

		String keyBase = "testKey";
		String object = "This is a test of an object blah blah es, serialization does not seem to slow things down so much.  The gzip compression is horrible horrible performance, so we only use it for very large objects.  I have not done any heavy benchmarking recently";

		long begin = System.currentTimeMillis();
		for (int i = start; i < start + runs; i++) {
			mc.set(keyBase + i, object);
		}
		long end = System.currentTimeMillis();
		long time = end - begin;
		System.out.println(runs + " sets: " + time + "ms");

		begin = System.currentTimeMillis();
		for (int i = start; i < start + runs; i++) {
			mc.get(keyBase + i);
		}
		end = System.currentTimeMillis();
		time = end - begin;
		System.out.println(runs + " gets: " + time + "ms");

		String[] keys = new String[runs];
		int j = 0;
		for (int i = start; i < start + runs; i++) {
			keys[j] = keyBase + i;
			j++;
		}
		begin = System.currentTimeMillis();
		mc.getMulti(keys);
		end = System.currentTimeMillis();
		time = end - begin;
		System.out.println(runs + " getMulti: " + time + "ms");

		begin = System.currentTimeMillis();
		for (int i = start; i < start + runs; i++) {
			mc.delete(keyBase + i);
		}
		end = System.currentTimeMillis();
		time = end - begin;
		System.out.println(runs + " deletes: " + time + "ms");

		SchoonerSockIOPool.getInstance("test").shutDown();
	}

}
