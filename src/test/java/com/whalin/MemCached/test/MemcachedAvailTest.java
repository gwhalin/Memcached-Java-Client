/**
 * Copyright (c) 2008 Greg Whalin
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
 * @author Greg Whalin <greg@meetup.com> 
 */
package com.whalin.MemCached.test;

import java.util.Random;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

public class MemcachedAvailTest {

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

		String[] serverlist = { "dev12:11211" };

		int threadNum = Integer.parseInt(args[0]);
		int runs = Integer.parseInt(args[1]);
		int size = 1024 * Integer.parseInt(args[2]); // how many kilobytes

		// initialize the pool for memcache servers
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(serverlist);

		pool.initialize();

		// get object to store
		int[] obj = new int[size];
		for (int i = 0; i < size; i++) {
			obj[i] = i;
		}

		String[] keys = new String[size];
		for (int i = 0; i < size; i++) {
			keys[i] = "test_key" + i;
		}
		Bench[] bens = new Bench[threadNum];
		for (int i = 0; i < threadNum; i++) {
			bens[i] = new Bench(runs, i, obj, keys);
			bens[i].start();
		}
		for (Bench ben : bens) {
			try {
				ben.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		pool.shutDown();
	}

	/**
	 * Test code per thread.
	 */
	private static class Bench extends Thread {
		private int runs;
		private int threadNum;
		private int[] object;
		private String[] keys;
		private int size;

		public Bench(int runs, int threadNum, int[] object, String[] keys) {
			this.runs = runs;
			this.threadNum = threadNum;
			this.object = object;
			this.keys = keys;
			this.size = object.length;
		}

		public void run() {

			// get client instance
			MemCachedClient mc = new MemCachedClient();

			// time stores
			long start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.set(keys[i], object);
			}
			long elapse = System.currentTimeMillis() - start;
			float avg = (float) elapse / runs;
			System.out.println("thread " + threadNum + ": runs: " + runs + " stores of obj " + (size / 1024)
					+ "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");
			Random r = new Random();
			for (int i = 0; i < runs; i++) {
				try {
					Thread.sleep(r.nextInt(10));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				start = System.nanoTime();
				mc.get(keys[i]);
				elapse = (System.nanoTime() - start) / 1000000;
				if (elapse > avg * 3)
					System.err.println(elapse + "ms for get in " + Thread.currentThread().getName());
			}

		}
	}
}
