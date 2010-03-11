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
package com.schooner.MemCached.command;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import com.danga.MemCached.Logger;
import com.danga.MemCached.MemCachedClient;
import com.schooner.MemCached.NativeHandler;
import com.schooner.MemCached.ObjectTransCoder;
import com.schooner.MemCached.SchoonerSockIO;
import com.schooner.MemCached.SockOutputStream;
import com.schooner.MemCached.TransCoder;

/**
 * This command implements the set command using memcached UDP protocol.
 * 
 * @author Meng Li
 * @since 2.5.0
 * @see com.schooner.Memcached.StorageCommand
 */
public class StorageCommand extends Command {

	public static Logger log = Logger.getLogger(StorageCommand.class.getName(), Logger.LEVEL_FATAL);
	public static final byte[] STORED = "STORED\r\n".getBytes();
	public static final byte[] NOT_STORED = "NOT_STORED\r\n".getBytes();
	public final byte[] BLAND_DATA_SIZE = "       ".getBytes();
	public static final byte[] B_RETURN = "\r\n".getBytes();

	private int flags;

	private TransCoder transCoder = new ObjectTransCoder();

	private Object value;

	private int valLen = 0;

	private int offset;

	private Long casUnique;

	/**
	 * set request textline:
	 * "set <key> <key> <flags> <exptime> <bytes> [noreply]\r\n"
	 * 
	 */
	public StorageCommand(String cmdname, String key, Object value, Date expiry, Integer hashCode, Long casUnique) {
		init(cmdname, key, value, expiry, hashCode, casUnique);
	}

	/**
	 * set request textline:
	 * "set <key> <key> <flags> <exptime> <bytes> [noreply]\r\n"
	 * 
	 */
	public StorageCommand(String cmdname, String key, Object value, Date expiry, Integer hashCode, Long casUnique,
			TransCoder transCoder) {
		init(cmdname, key, value, expiry, hashCode, casUnique);
		this.transCoder = transCoder;
	}

	private void init(String cmdname, String key, Object value, Date expiry, Integer hashCode, Long casUnique) {
		// store flags
		flags = NativeHandler.getMarkerFlag(value);
		// construct the command
		String cmd = new StringBuffer().append(cmdname).append(" ").append(key).append(" ").append(flags).append(" ")
				.append(expiry.getTime() / 1000).append(" ").toString();

		textLine = cmd.getBytes();

		this.value = value;
		this.casUnique = casUnique;
	}

	private boolean writeDataBlock(SchoonerSockIO sock) throws IOException {
		SockOutputStream output = new SockOutputStream(sock);
		if (flags != MemCachedClient.MARKER_OTHERS) {
			/*
			 * Using NativeHandler to serialize the value
			 */
			byte[] b = NativeHandler.encode(value);
			output.write(b);
			valLen = b.length;
		} else {
			/*
			 * Using default object transcoder to serialize the non-primitive
			 * values.
			 */
			valLen = transCoder.encode(output, value);
		}

		sock.writeBuf.put(B_RETURN);

		byte[] objectSize = new Integer(valLen).toString().getBytes();
		int oldPosition = sock.writeBuf.position();
		sock.writeBuf.position(offset);
		// put real object bytes size
		sock.writeBuf.put(objectSize);
		// return to correct position.
		sock.writeBuf.position(oldPosition);

		return true;
	}

	public short request(SchoonerSockIO sock) throws IOException {
		short rid = sock.preWrite();
		sock.writeBuf.put(textLine);

		offset = sock.writeBuf.position();
		// write blank bytes size.
		sock.writeBuf.put(BLAND_DATA_SIZE);
		if (casUnique != 0)
			sock.writeBuf.put((" " + casUnique.toString()).getBytes());

		sock.writeBuf.put(B_RETURN);

		if (value != null) {
			writeDataBlock(sock);
		}

		sock.writeBuf.flip();
		sock.getByteChannel().write(sock.writeBuf);

		return rid;
	}

	public boolean response(SchoonerSockIO sock, short rid) throws IOException {
		byte[] temp = sock.getResponse(rid);// new
		// byte[sock.readBuf.position()];

		if (Arrays.equals(STORED, temp)) {
			/*
			 * Successfully set here.
			 */
			return true;
		}

		return false;
	}

}
