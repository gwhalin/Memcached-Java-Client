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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.whalin.MemCached.MemCachedClient;

public class MemcachedClientAdvChecker {

	private static final int GET = 0;
	private static final int SET = 1;
	private static final int DELETE = 2;
	private static final int ADD = 3;
	private static final int APPEND = 4;
	private static final int PREPEND = 5;

	private static final int MAX_QUEUE_SIZE = 1024;

	private static final String characterSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

	private static final Map<String, Integer> functionMap = new HashMap<String, Integer>();

	private static double[] range;
	private static String[] rangeOps;

	private static int thread;
	private static long runtime;
	private static int sleep;
	private static long checkInterval;

	private static Thread[] threads;

	static {
		functionMap.put("get", GET);
		functionMap.put("set", SET);
		functionMap.put("delete", DELETE);
		functionMap.put("add", ADD);
		functionMap.put("append", APPEND);
		functionMap.put("prepend", PREPEND);
	}

	private static synchronized String initString(int len) {
		StringBuffer sb = new StringBuffer(len);
		Random random = new Random();
		int size = characterSet.length();
		for (int i = 0; i < len; ++i) {
			sb.append(characterSet.charAt(random.nextInt(size)));
		}
		return sb.toString();
	}
	
	private static class TestThread extends Thread {

		private long start = System.currentTimeMillis();
		private MemCachedClient mc;
		private Queue<String> keys = new ConcurrentLinkedQueue<String>();
		private String keyBase = getName() + "_" + getId();
		private AtomicInteger current = new AtomicInteger(0);
		private long lastCheck = System.currentTimeMillis();
		
		public TestThread(String kind) {
			super();
			if (kind.equals("ascii"))
				mc = new MemCachedClient(true, false);
			else if (kind.equals("binary"))
				mc = new MemCachedClient(true, true);
			else if (kind.equals("udp"))
				mc = new MemCachedClient(false, false);
		}
		
		public void run() {
			if (runtime > 0) {
				while (System.currentTimeMillis() - start < runtime) {
					doCheck();
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} else {
				while (true) {
					doCheck();
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		synchronized private void checkData(MemCachedClient mc) {
			String k = keys.element();
			if (mc.get(k) == null) {
				System.err.println("CHECK " + k + "ERROR! SEE advChecker_errors.log");
			}
		}

		private void doCheck() {
			double ran;
			ran = Math.random();
			for (int i = 0; i < range.length; i++) {
				if (ran <= range[i]) {
					doCommand(functionMap.get(rangeOps[i]));
					if (System.currentTimeMillis() - lastCheck > checkInterval) {
						lastCheck = System.currentTimeMillis();
						checkData(mc);
					}
					break;
				}
			}
		}

		private void doCommand(Integer integer) {
			switch (integer.intValue()) {
			case GET:
				String k = null;
				if (keys.size() > 0) {
					k = keys.element();
					if (mc.get(k) == null)
						System.err.println("Get Failed!");
				}
				break;
			case SET:
				String key = keyBase + current.addAndGet(1);
				if (!mc.set(key, initString(1024)))
					System.err.println("Set Failed!");
				else { 
					if (keys.size() >= MAX_QUEUE_SIZE) {
						keys.remove();
					}
					keys.add(key);
				}
				break;
			case DELETE:
				if (keys.size() > 0) {
					k = keys.element();
					keys.remove();
					if (!mc.delete(k))
						System.err.println("Delete Failed!");
				}
				break;
			case ADD:
				if (keys.size() > 0) {
					k = keys.element();
					if (mc.add(k, initString(1024)))
						System.err.println("Add Failed!");
				}
				break;
			case APPEND:
				if (keys.size() > 0) {
					k = keys.element();
					if (!mc.append(k, initString(1024)))
						System.err.println("Append Failed!");
				}
				break;
			case PREPEND:
				if (keys.size() > 0) {
					k = keys.element();
					if (!mc.prepend(k, initString(1024)))
						System.err.println("Perpend Failed!");
				}
				break;
			}
		}
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage MemcachedClientAdvChecker <config_file_name> <hostname1:port1> ascii|binary|udp");
			System.exit(1);
		}

		// create DocumentBuilderFactory
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		// create DocumentBuilder
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
			// obtain document object from XML document
			Document document = builder.parse(new File(args[0]));

			// get root node
			Element root = document.getDocumentElement();
			NodeList childNodes = root.getElementsByTagName("op");

			thread = Integer.parseInt(root.getAttribute("thread"));
			checkInterval = Integer.parseInt(root.getAttribute("checkInterval"));
			int tr = Integer.parseInt(root.getAttribute("runtime"));
			if (tr > 0)
				runtime = tr * 60 * 1000000L;
			else
				runtime = -1;
			sleep = 1;// Integer.parseInt(root.getAttribute("sleep"));

			setupRange(childNodes);

			threads = new Thread[thread];
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String[] servers = args[1].split(",");
		String kind = args[2];
		SchoonerSockIOPool pool = null;
		if (kind.equals("ascii"))
			pool = SchoonerSockIOPool.getInstance(true);
		else if (kind.equals("binary"))
			pool = SchoonerSockIOPool.getInstance(true);
		else if (kind.equals("udp"))
			pool = SchoonerSockIOPool.getInstance(false);
		pool.setServers(servers);
		pool.setNagle(false);
		pool.setInitConn(8);
		pool.initialize();

		for (int th = 0; th < thread; ++th) {
			threads[th] = new TestThread(kind);
			threads[th].start();
		}

		for (int th = 0; th < thread; ++th) {
			try {
				threads[th].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static void setupRange(NodeList childNodes) {
		double r = 0;
		range = new double[childNodes.getLength()];
		rangeOps = new String[childNodes.getLength()];

		for (int i = 0; i < childNodes.getLength(); i++) {
			Node currentNode = childNodes.item(i);
			rangeOps[i] = currentNode.getAttributes().getNamedItem("name").getNodeValue();
			r += Integer.parseInt(currentNode.getAttributes().getNamedItem("percent").getNodeValue()) / 100.0;
			range[i] = r;
		}
	}

}
