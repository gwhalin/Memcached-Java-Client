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

import com.danga.MemCached.Logger;
import com.schooner.MemCached.SchoonerSockIO;

/**
 * Increase a digital value in memcached server.
 * 
 * @author Qinliang Lin
 * @see com.schooner.MemCached.command.IncrdecrCommand
 * @since 2.5.0
 */
public class IncrdecrCommand extends Command {

	public static Logger log = Logger.getLogger(IncrdecrCommand.class.getName(), Logger.LEVEL_FATAL);
	public static final String NOTFOUND = "NOT_FOUND\r\n";

	private Long result;

	public IncrdecrCommand(String cmdname, String key, long inc, Integer hashCode) {
		String cmd = new StringBuffer().append(cmdname).append(" ").append(key).append(" ").append(inc).append("\r\n")
				.toString();
		textLine = cmd.getBytes();
	}

	public boolean response(SchoonerSockIO sock, short rid) throws IOException {
		byte[] res = sock.getResponse(rid);
		String line = new String(res).split("\r\n")[0];
		if (line.matches("\\d+")) {
			// Sucessfully increase.
			// return sock to pool and return result
			try {
				result = Long.parseLong(line);
				return true;
			} catch (Exception ex) {
				log.error(new StringBuffer().append("Failed to parse Long value for key: ").toString());
			}
		}

		return false;
	}

	public Long getResult() {
		return result;
	}

}
