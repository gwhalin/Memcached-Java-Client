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
import com.schooner.MemCached.SchoonerSockIO;

/**
 * The command that delete an item in memcached server.
 * 
 * @author Qinliang Lin
 * @see com.schooner.MemCached.command.DeletionCommand
 * @since 2.5.0
 */
public class DeletionCommand extends Command {

	private static Logger log = Logger.getLogger(DeletionCommand.class.getName(), Logger.LEVEL_FATAL);
	private static final byte[] DELETED = "DELETED\r\n".getBytes();
	private static final byte[] NOTFOUND = "NOT_FOUND\r\n".getBytes();

	/**
	 * deletion request textline: "delete <key> [<time>] [noreply]\r\n"
	 * 
	 */
	public DeletionCommand(String key, Integer hashCode, Date expiry) {
		StringBuilder command = new StringBuilder("delete").append(DELIMITER).append(key);
		if (expiry != null)
			command.append(" " + expiry.getTime() / 1000);
		command.append("\r\n");
		textLine = command.toString().getBytes();
	}

	public boolean response(SchoonerSockIO sock, short rid) throws IOException {
		byte[] res = sock.getResponse(rid);
		if (Arrays.equals(res, DELETED)) {
			if (log.isInfoEnabled())
				log.info("DELETED!");
			return true;
		} else if (Arrays.equals(res, NOTFOUND)) {
			if (log.isInfoEnabled())
				log.info("NOT_FOUND!");
		} else {
			log.error("error");
		}
		return false;
	}
}
