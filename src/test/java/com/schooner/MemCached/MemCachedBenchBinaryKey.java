package com.schooner.MemCached;

public class MemCachedBenchBinaryKey extends MemCachedBenchTcpKey {
	protected String FILE_URL = "spring-memcached-Binary.xml";

	@Override
	public String getFileUrl() {
		return FILE_URL;
	}
}
