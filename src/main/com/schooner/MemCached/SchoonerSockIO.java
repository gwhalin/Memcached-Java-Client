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

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.danga.MemCached.SockIOPool;

/**
 * An adapter of com.danga.MemCached.SockIOPool.SockIO.
 * 
 * @author Xingen Wang
 * @since 2.5.0
 * @see SchoonerSockIO
 * @see com.danga.MemCached.SockIOPool.SockIO
 */
public abstract class SchoonerSockIO extends SockIOPool.SockIO {

	public SchoonerSockIO(int bufferSize) throws UnknownHostException, IOException {
		super(null, null, 0, 0, false);
		this.bufferSize = bufferSize;
	}

	private int bufferSize = 1024 * 1025;

	// the datagram sent from memcached mustn't beyond 1400 bytes.
	public ByteBuffer readBuf = ByteBuffer.allocateDirect(8 * 1024);
	public ByteBuffer writeBuf;

	protected boolean isPooled = true;
	
	protected ConcurrentLinkedQueue<SchoonerSockIO> sockets;

	protected AtomicInteger sockNum;

	public abstract short preWrite();

	public abstract byte[] getResponse(short rid) throws IOException;

	/**
	 * get byte channel from this socket.
	 * 
	 * @return the backing SocketChannel
	 */
	public abstract ByteChannel getByteChannel();

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		writeBuf = ByteBuffer.allocateDirect(this.bufferSize);
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public void setPooled(boolean isPooled) {
		this.isPooled = isPooled;
	}

	public boolean isPooled() {
		return isPooled;
	}
	
	public AtomicInteger getSockNum() {
		return sockNum;
	}

	public void setSockNum(AtomicInteger sockNum) {
		this.sockNum = sockNum;
	}
	
}
