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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;

/**
 * {@link SockInputStream} is a inputstream based on a socket. Due to memcached
 * protocol, we can only read specified length from the socket, you have to set
 * the byte length to read before invoke read.
 * 
 * @author Xingen Wang
 * @since 2.5.0
 * @see InputStream
 */
public final class SockInputStream extends InputStream {
	private SchoonerSockIO sock;
	private int limit;
	private int count = 0;

	/**
	 * get length of to-be-read.
	 * 
	 * @return length
	 */
	public final int getWillRead() {
		return limit;
	}

	/**
	 * set limited length to read.
	 * 
	 * @param length
	 *            length to set.
	 */
	public final void willRead(int limit) {
		this.limit = limit;
		count = 0;
	}

	/**
	 * Constructor.
	 * 
	 * @param sock
	 *            {@link SchoonerSockIO}, read from this socket.
	 * @param limit
	 *            limited length to read from specified socket.
	 * @throws IOException
	 *             error happened in reading.
	 */
	public SockInputStream(final SchoonerSockIO sock, int limit) throws IOException {
		this.sock = sock;
		willRead(limit);
		sock.readBuf.clear();
		sock.getChannel().read(sock.readBuf);
		sock.readBuf.flip();
	}

	/**
	 * Constructor. Read all buffered bytes in the socket.
	 * 
	 * @param sock
	 *            {@link SchoonerSockIO}, read from this socket.
	 * @throws IOException
	 *             error happened in reading.
	 */
	public SockInputStream(final SchoonerSockIO sock) throws IOException {
		this(sock, sock.readBuf.remaining());
	}

	@Override
	public final int read() throws IOException {
		if (count >= limit) {
			return -1;
		}
		byte b = 0;
		try {
			b = sock.readBuf.get();
		} catch (BufferUnderflowException e) {
			readFromChannel();
			b = sock.readBuf.get();
		}
		count++;
		return b & 0xff;
	}

	/**
	 * read bytes from socket.
	 * 
	 * @throws IOException
	 *             error happened in read.
	 */
	private final void readFromChannel() throws IOException {
		sock.readBuf.clear();
		sock.getChannel().read(sock.readBuf);
		sock.readBuf.flip();
	}

	@Override
	public final int read(byte[] b, int off, int len) throws IOException {
		if (count >= limit) {
			return -1;
		}
		int read = 0;
		int r1 = 0;
		final int remain = limit - count;
		len = len < remain ? len : remain;
		while (len - read > 0) {
			r1 = sock.readBuf.remaining();
			r1 = r1 < len - read ? r1 : len - read;
			sock.readBuf.get(b, off + read, r1);
			if (r1 != len - read)
				readFromChannel();
			read += r1;
		}
		count += len;
		return len;
	}

	/**
	 * get all left bytes from the socket.
	 * 
	 * @return byte array of holding all bytes.
	 * @throws IOException
	 *             error happened in reading.
	 */
	public final byte[] getBuffer() throws IOException {
		byte[] bs = new byte[limit - count];
		read(bs);
		return bs;
	}

	/**
	 * get a line from the socket.
	 * 
	 * @return a line
	 * @throws IOException
	 *             error happend in reading.
	 * @since 2.5.1
	 */
	public final String getLine() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		int b;
		while ((b = read()) != -1) {
			bos.write(b);
			if (b == '\n') {
				break;
			}
		}
		return new String(bos.toByteArray());
	}

	@Override
	public int available() throws IOException {
		return sock.readBuf.remaining();
	}

}
