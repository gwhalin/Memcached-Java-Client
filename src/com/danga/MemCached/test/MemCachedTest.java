/**
 * MemCachedTest.java
 * Test class for testing memcached java client.
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

public class MemCachedTest {

	// store results from threads
	private static Hashtable threadInfo = new Hashtable();
    
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

		String[] serverlist = { "cache1.int.meetup.com:12345", "cache0.int.meetup.com:12345" };

		// initialize the pool for memcache servers
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(serverlist);

		pool.setInitConn(5);
		pool.setMinConn(5);
		pool.setMaxConn(50);
		pool.setMaintSleep(30);

		pool.setNagle(false);
		pool.initialize();

		int threads = Integer.parseInt(args[0]);
		int runs = Integer.parseInt(args[1]);
		int size = 1024 * Integer.parseInt(args[2]);	// how many kilobytes

		// get object to store
		int[] obj = new int[size];
		for (int i = 0; i < size; i++) {
			obj[i] = i;
		}

		String[] keys = new String[size];
		for (int i = 0; i < size; i++) {
			keys[i] = "test_key" + i;
		}

		for (int i = 0; i < threads; i++) {
			bench b = new bench(runs, i, obj, keys);
			b.start();
		}

		int i = 0;
		while (i < threads) {
			if (threadInfo.containsKey(new Integer(i))) {
				System.out.println((StringBuffer) threadInfo.get(new Integer(i)));
				i++;
			} else {
				try {
					Thread.currentThread().sleep(1000);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		pool.shutDown();
		System.exit(1);
	}

	/** 
	 * Test code per thread. 
	 */
	private static class bench extends Thread {
		private int runs;
		private int threadNum;
		private int[] object;
		private String[] keys;
		private int size;

		public bench(int runs, int threadNum, int[] object, String[] keys) {
			this.runs = runs;
			this.threadNum = threadNum;
			this.object = object;
			this.keys = keys;
			this.size = object.length;
		}

		public void run() {

			StringBuffer result = new StringBuffer();

			// get client instance
			MemCachedClient mc = new MemCachedClient();
			mc.setCompressEnable(false);
			mc.setCompressThreshold(0);

			// time deletes
			long start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.delete(keys[i]);
			}
			long elapse = System.currentTimeMillis() - start;
			float avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs + " deletes of obj " + (size/1024) + "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");

			// time stores
			start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.set(keys[i], object);
			}
			elapse = System.currentTimeMillis() - start;
			avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs + " stores of obj " + (size/1024) + "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");

			start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.get(keys[i]);
			}
			elapse = System.currentTimeMillis() - start;
			avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs + " gets of obj " + (size/1024) + "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");

			threadInfo.put(new Integer(threadNum), result);
		}
	}
}
