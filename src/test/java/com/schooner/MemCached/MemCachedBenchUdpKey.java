package com.schooner.MemCached;

public class MemCachedBenchUdpKey extends MemCachedBenchTcpKey {
	protected String FILE_URL = "spring-memcached-UDP.xml";

	@Override
	public String getFileUrl() {
		return FILE_URL;
	}
}
