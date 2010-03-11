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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import com.danga.MemCached.Logger;
import com.schooner.MemCached.AscIIUDPClient;
import com.schooner.MemCached.MemcachedItem;
import com.schooner.MemCached.NativeHandler;
import com.schooner.MemCached.SchoonerSockIO;
import com.schooner.MemCached.TransCoder;

/**
 * Retrieve a item from memcached server.
 * 
 * @author Meng Li
 * @since 2.5.0
 * @see com.schooner.MemCached.command.RetrievalCommand
 */
public class RetrievalCommand extends Command {

	private static Logger log = Logger.getLogger(RetrievalCommand.class.getName(), Logger.LEVEL_FATAL);

	private static final byte[] B_END = "END\r\n".getBytes();
	private static final byte[] B_VALUE = "VALUE ".getBytes();
	private String key;
	private String cmd;

	/**
	 * 
	 * request: "get <key>*\r\n" or "gets <key>*\r\n"
	 * 
	 * response: "[item]*END\r\n"
	 * 
	 * item: "VALUE <key> <flags> <bytes> [<cas unique>]\r\n<data block>\r\n"
	 * 
	 * 
	 * @param get
	 *            or gets
	 * @param key
	 * @param hashCode
	 * 
	 */
	public RetrievalCommand(String cmd, String key) {
		this.key = key;
		this.cmd = cmd;
		StringBuilder command = new StringBuilder(cmd).append(DELIMITER).append(key).append(RETURN);
		textLine = command.toString().getBytes();
	}

	public class Value {
		public int flags;
		public int bytes;
		public long casUnique;
		public byte[] dataBlock;
	}

	public class ResponseParser {
		private LinkedList<Value> list;
		public Value retvalue = null;

		public void exec(byte[] res) throws IOException {
			ByteArrayInputStream stream = new ByteArrayInputStream(res);
			StringBuilder sb = new StringBuilder();
			byte[] end = new byte[5];
			byte next;
			int length = 0;
			int i = 0;

			// check if it is the end.
			stream.mark(0);
			stream.read(end);
			if (Arrays.equals(end, B_END)) {
				return;
			}
			stream.reset();

			while (true) {
				Value value = new Value();
				// skip "VALUE <key> "
				stream.skip(B_VALUE.length + key.length() + 1);

				// get the length of <flags> and build it.
				length = 0;
				while ((next = (byte) stream.read()) != AscIIUDPClient.B_DELIMITER) {
					length++;
					sb.append((char) next);
				}
				try {
					value.flags = Integer.valueOf(sb.toString());
				} catch (NumberFormatException e) {
					retvalue = null;
					return;
				}
				sb.delete(0, length);

				// get the length of <byte> and build it.
				length = 0;
				while (((next = (byte) stream.read()) != AscIIUDPClient.B_DELIMITER) && (next != B_RETURN)) {
					length++;
					sb.append((char) next);
				}

				try {
					value.bytes = Integer.valueOf(sb.toString());
				} catch (NumberFormatException e) {
					retvalue = null;
					return;
				}
				sb.delete(0, length);

				if (cmd.equals("gets")) {
					// if gets then get the length of <casUnique> and build it.
					length = 0;
					while ((next = (byte) stream.read()) != B_RETURN) {
						length++;
						sb.append((char) next);
					}
					try {
						value.casUnique = Long.valueOf(sb.toString());
					} catch (NumberFormatException e) {
						retvalue = null;
						return;
					}
					sb.delete(0, length);
				}

				// skip "\n"
				stream.skip(1);

				// build datablock
				value.dataBlock = new byte[value.bytes];
				stream.read(value.dataBlock);

				// skip "\r\n"
				stream.skip(2);

				// check if it is the end.
				stream.mark(0);
				stream.read(end);
				if (Arrays.equals(end, B_END)) {
					retvalue = value;
					break;
				} else {
					stream.reset();
				}
				if (i == 0) {
					list = new LinkedList<Value>();
				}
				list.add(value);
				i++;
			}
		}
	}

	public MemcachedItem response(SchoonerSockIO sock, TransCoder transCoder, short rid) throws IOException {
		byte[] res = sock.getResponse(rid);
		MemcachedItem item = new MemcachedItem();

		if (res == null)
			return item;

		ResponseParser parser = new ResponseParser();
		parser.exec(res);
		if (parser.retvalue != null) {
			Value value = parser.retvalue;
			if (cmd.equals("gets")) {
				item.casUnique = value.casUnique;
			}
			try {
				if (NativeHandler.isHandled(value.flags)) {
					item.value = NativeHandler.decode(value.dataBlock, value.flags);
				} else if (transCoder != null) {
					// decode object with default transcoder.
					item.value = transCoder.decode(new ByteArrayInputStream(value.dataBlock));
				}
			} catch (IOException e) {
				log.error("error happend in decoding the object");
				throw e;
			}
			return item;
		}
		// TODO: for get multi only.
		return item;
	}
}
