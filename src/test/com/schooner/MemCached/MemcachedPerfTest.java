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

public class MemcachedPerfTest {

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
		pool.setMaxConn(Integer.parseInt(args[5]));
		pool.setMinConn(Integer.parseInt(args[4]));
		pool.setMaxIdle(Integer.parseInt(args[6]));
		pool.setNagle(false);
		pool.initialize();

		int threads = Integer.parseInt(args[1]);
		int runs = Integer.parseInt(args[2]);
		int size = 1024 * Integer.parseInt(args[3]) / 4; // how many kilobytes

		// get object to store
		int[] obj = new int[size];
		for (int i = 0; i < size; i++) {
			obj[i] = i;
		}
		bench[] b = new bench[threads];
		for (int i = 0; i < threads; i++) {
			b[i] = new bench(runs, obj, args[0]);
		}

		long start, elapse;
		float avg;
		start = System.currentTimeMillis();
		for (int i = 0; i < threads; i++) {
			b[i].start();
		}
		for (int i = 0; i < threads; i++) {
			try {
				b[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		elapse = System.currentTimeMillis() - start;
		avg = (float) elapse * 1000 / (runs * threads);
		System.out.println(args[0] + " runs: " + runs * threads + " stores of obj " + (size / 1024)
				+ "KB -- avg time per req " + avg + " us (total: " + elapse + " ms)");

		pool.shutDown();
		System.exit(1);
	}

	/**
	 * Test code per thread.
	 */
	private static class bench extends Thread {
		private int runs;
		private String threadName;
		private int[] object;
		private String opKind;

		public bench(int runs, int[] object, String opKind) {
			this.runs = runs;
			this.threadName = getName();
			this.object = object;
			this.opKind = opKind;
		}

		public void run() {
			// get client instance
			MemCachedClient mc = new MemCachedClient("test");

			if (opKind.equals("set")) {
				// time stores
				for (int i = 0; i < runs; i++) {
					mc.set(threadName + "_" + i, object);
				}
			} else if (opKind.equals("get")) {
				for (int i = 0; i < runs; i++) {
					mc.get(threadName + "_" + i);
				}
			} else if (opKind.equals("delete")) {
				// time deletes
				for (int i = 0; i < runs; i++) {
					mc.delete(threadName + "_" + i);
				}
			} else if (opKind.equals("getMulti")) {
				// time deletes
				for (int i = 0; i < runs; i++) {
					mc.getMulti(new String[] { threadName + "_" + 0, threadName + "_" + i });
				}
			}
		}
	}
}
