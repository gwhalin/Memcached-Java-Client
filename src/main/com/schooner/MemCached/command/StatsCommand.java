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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import com.danga.MemCached.Logger;
import com.schooner.MemCached.SchoonerSockIO;

/**
 * Get the state of memcached server.
 * 
 * @author Qinliang Lin
 * @see com.schooner.MemCached.command.StatsCommand
 * @since 2.5.0
 */
public class StatsCommand extends Command {

	public static Logger log = Logger.getLogger(StatsCommand.class.getName(), Logger.LEVEL_FATAL);
	public static final String END = "END\r\n";
	public static final String ERROR = "ERROR\r\n";
	public static final String CLIENT_ERROR = "CLIENT_ERROR\r\n";
	public static final String SERVER_ERROR = "SERVER_ERROR\r\n";

	private String lineStart;

	public StatsCommand(String command, String lineStart) {
		textLine = command.getBytes();
		this.lineStart = lineStart;
	}

	public Map<String, String> response(SchoonerSockIO sock, short rid) throws IOException {
		Map<String, String> stats = new HashMap<String, String>();
		String line;
		byte[] temp = sock.getResponse(rid);
		BufferedReader reader = new BufferedReader(new StringReader(new String(temp)));
		// loop over results
		while ((line = reader.readLine()) != null) {
			if (line.startsWith(lineStart)) {
				String[] info = line.split(" ", 3);
				String key = info[1];
				String value = info[2];
				stats.put(key, value);
			} else if (END.equals(line)) {
				// finish when we get end from server
				break;
			} else if (line.startsWith(ERROR) || line.startsWith(CLIENT_ERROR) || line.startsWith(SERVER_ERROR)) {
				log.error("++++ failed to query stats");
				log.error("++++ server response: " + line);
				break;
			}
		}
		return stats;
	}

}
