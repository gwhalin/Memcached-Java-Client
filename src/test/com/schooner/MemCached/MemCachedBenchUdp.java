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

import com.danga.MemCached.MemCachedClient;

public class MemCachedBenchUdp {

	static {
		String servers = System.getProperty("memcached.host");
		String[] serverlist = servers.split(",");

		// initialize the pool for memcache servers
		SchoonerSockIOPool pool = SchoonerSockIOPool.getInstance("test", false);
		pool.setServers(serverlist);
		pool.setNagle(false);
		pool.setHashingAlg(SchoonerSockIOPool.CONSISTENT_HASH);
		pool.initialize();
	}

	// logger
	// private static Logger log =
	// Logger.getLogger(MemCachedBench.class.getName());

	public static void main(String[] args) {
		int runs = Integer.parseInt(args[0]);
		int start = Integer.parseInt(args[1]);

		// get client instance
		MemCachedClient mc = new MemCachedClient("test", false, false);

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
