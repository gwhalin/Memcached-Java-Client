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

/**
 * {@link AbstractTransCoder} is nearly the same as the interface
 * {@link TransCoder}, the only difference is that you needn't return the
 * written size for memcached set operation.
 * 
 * @author Xingen Wang
 * @since 2.5.0
 * @see TransCoder
 * @see ObjectTransCoder
 */
public abstract class AbstractTransCoder implements TransCoder {

	/*
	 * (non-Javadoc)
	 * 
	 * @seecom.schooner.MemCached.TransCoder#encode(com.schooner.MemCached.
	 * SockOutputStream, java.lang.Object)
	 */
	public int encode(SockOutputStream out, Object object) throws IOException {
		out.resetCount();
		encode((OutputStream) out, object);
		return out.getCount();
	}

	/**
	 * encode the java object into outputstream.
	 * 
	 * @param out
	 *            outputstream to hold the data.
	 * @param object
	 *            object to be encoded.
	 * @throws IOException
	 */
	public abstract void encode(OutputStream out, Object object) throws IOException;

}
