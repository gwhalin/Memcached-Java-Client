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

import java.util.Hashtable;

import com.danga.MemCached.MemCachedClient;

public class MemcachedPerfTest {

	// store results from threads
	private static Hashtable<Integer, StringBuilder> threadInfo = new Hashtable<Integer, StringBuilder>();

	/**
	 * This runs through some simple tests of the MemcacheClient.
	 * 
	 * Command line args: args[0] = number of threads to spawn args[1] = number
	 * of runs per thread args[2] = size of object to store
	 * 
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {

		String[] serverlist = { "localhost:11211", "localhost:11212" };

		// initialize the pool for memcache servers
		SchoonerSockIOPool pool = SchoonerSockIOPool.getInstance("test");
		pool.setServers(serverlist);

		// pool.setInitConn(5);
		// pool.setMinConn(5);
		pool.setMaxConn(50);
		// pool.setMaintSleep(30);

		pool.setNagle(false);
		pool.initialize();

		int threads = Integer.parseInt(args[0]);
		int runs = Integer.parseInt(args[1]);
		int size = 1024 * Integer.parseInt(args[2]); // how many kilobytes

		// get object to store
		int[] obj = new int[size];
		for (int i = 0; i < size; i++) {
			obj[i] = i;
		}

		for (int i = 0; i < threads; i++) {
			bench b = new bench(runs, i, obj);
			b.start();
		}

		int i = 0;
		while (i < threads) {
			if (threadInfo.containsKey(new Integer(i))) {
				System.out.println(threadInfo.get(new Integer(i)));
				i++;
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
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
		private int size;

		public bench(int runs, int threadNum, int[] object) {
			this.runs = runs;
			this.threadNum = threadNum;
			this.object = object;
			this.size = object.length;
		}

		public void run() {

			StringBuilder result = new StringBuilder();

			// get client instance
			MemCachedClient mc = new MemCachedClient("test");

			// time stores
			long start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.set(getName() + " " + i, object);
			}
			long elapse = System.currentTimeMillis() - start;
			float avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs + " stores of obj " + (size / 1024)
					+ "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");

			start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.get(getName() + " " + i);
			}
			elapse = System.currentTimeMillis() - start;
			avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs + " gets of obj " + (size / 1024)
					+ "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");

			// time deletes
			start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.delete(getName() + " " + i);
			}
			elapse = System.currentTimeMillis() - start;
			avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs + " deletes of obj " + (size / 1024)
					+ "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");

			threadInfo.put(new Integer(threadNum), result);
		}
	}
}
