/**
 * MemCachedBench.java
 *
 * Copyright (c) 2005
 * Greg Whalin <greg@meetup.com>
 *
 * See the memcached website:
 * http://www.danga.com/memcached/
 *
 * This module is Copyright (c) 2005 Greg Whalin.
 * All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later
 * version.
 *
 * This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 * @author Greg Whalin <greg@meetup.com> 
 * @version 1.2
 */
package com.danga.MemCached.test;

import com.danga.MemCached.*;
import java.util.*;

public class MemCachedBench {

	public static void main(String[] args) {

		int runs = Integer.parseInt(args[0]);
		int start = Integer.parseInt(args[1]);

		String[] serverlist = { "cache0.int.meetup.com:1624" };

		// initialize the pool for memcache servers
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(serverlist);

		pool.setInitConn(100);
		pool.setMinConn(100);
		pool.setMaxConn(500);
		pool.setMaintSleep(30);

		pool.setNagle(false);
		pool.initialize();

		// get client instance
		MemCachedClient mc = new MemCachedClient();
		mc.setCompressEnable(false);

		String keyBase = "testKey";
		String object = "This is a test of an object blah blah es, serialization does not seem to slow things down so much.  The gzip compression is horrible horrible performance, so we only use it for very large objects.  I have not done any heavy benchmarking recently";

		long begin = System.currentTimeMillis();
		for (int i = start; i < start+runs; i++) {
			mc.set(keyBase + i, object);
		}
		long end = System.currentTimeMillis();
		long time = end - begin;

		System.out.println(runs + " gets: " + time + "ms");

		begin = System.currentTimeMillis();
		for (int i = start; i < start+runs; i++) {
			String str = (String) mc.get(keyBase + i);
		}
		end = System.currentTimeMillis();
		time = end - begin;

		System.out.println(runs + " gets: " + time + "ms");
		SockIOPool.getInstance().shutDown();
	}
}
