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
import java.io.OutputStream;
import java.nio.BufferOverflowException;

/**
 * {@link SockOutputStream} is a outputstream based on socket. There will be a
 * big buffer in the socket, which is 1Mb in default, you can put all bytes into
 * this buffer easily, when it is full, it will flush the bytes into the socket.<br>
 * Don't make the buffer too much small, or else you will have problem in
 * memcached set operation.
 * 
 * @author Xingen Wang
 * @since 2.5.0
 * @see OutputStream
 * @see SchoonerSockIO
 */
public final class SockOutputStream extends OutputStream {
	private int count = 0;
	private SchoonerSockIO sock;

	/**
	 * get byte count, how many wrote.
	 * 
	 * @return
	 */
	public final int getCount() {
		return count;
	}

	/**
	 * reset the count;
	 */
	public final void resetCount() {
		this.count = 0;
	}

	/**
	 * get channel of this stream.
	 * 
	 * @return channel specified with this stream.
	 */
	public final SchoonerSockIO getSock() {
		return sock;
	}

	/**
	 * Constructor with SockIO.
	 * 
	 * @param sock
	 *            specified socket for this outputstream.
	 */
	public SockOutputStream(final SchoonerSockIO sock) {
		this.sock = sock;
	}

	/**
	 * write buffered bytes into socket channel.
	 * 
	 * @throws IOException
	 *             error happened.
	 */
	private final void writeToChannel() throws IOException {
		sock.writeBuf.flip();
		sock.getChannel().write(sock.writeBuf);
		sock.writeBuf.clear();
	}

	@Override
	public final void write(int b) throws IOException {
		try {
			sock.writeBuf.put((byte) b);
		} catch (BufferOverflowException e) {
			writeToChannel();
			sock.writeBuf.put((byte) b);
		}
		count++;
	}

	@Override
	public final void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public final void write(byte[] b, int off, int len) throws IOException {
		if (len == 0)
			return;
		if (sock.writeBuf.remaining() >= len)
			sock.writeBuf.put(b, off, len);
		else {
			int written = 0;
			int w1 = 0;
			int tRemain = 0;
			while ((tRemain = len - written) > 0) {
				w1 = sock.writeBuf.remaining();
				w1 = w1 < tRemain ? w1 : tRemain;
				if (w1 == 0)
					writeToChannel();
				else
					sock.writeBuf.put(b, off, w1);
				written += w1;
			}
		}
		count += len;
	}
}
