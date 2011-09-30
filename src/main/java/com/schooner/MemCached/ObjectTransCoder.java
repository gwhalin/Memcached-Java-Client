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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import com.danga.MemCached.ContextObjectInputStream;

/**
 * {@link ObjectTransCoder} is the default TransCoder used to handle the
 * serialization and deserialization in memcached operations.
 * 
 * @author Xingen Wang
 * @since 2.5.0
 * @see AbstractTransCoder
 * @see TransCoder
 */
public class ObjectTransCoder extends AbstractTransCoder {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.schooner.MemCached.TransCoder#decode(InputStream)
	 */
	public Object decode(final InputStream input) throws IOException {
		Object obj = null;
		ObjectInputStream ois = new ObjectInputStream(input);
		try {
			obj = ois.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e.getMessage());
		}
		ois.close();
		return obj;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.schooner.MemCached.AbstractTransCoder#encode(java.io.OutputStream,
	 * java.lang.Object)
	 */
	public void encode(final OutputStream output, final Object object) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(output);
		oos.writeObject(object);
		oos.close();
	}

	/**
	 * decode the object from the inputstream with your classloader
	 * 
	 * @param input
	 *            inputstream.
	 * @param classLoader
	 *            speicified classloader created by you.
	 * @return decoded java object.
	 * @throws IOException
	 *             error happened in decoding the input stream.
	 */
	public Object decode(InputStream input, ClassLoader classLoader) throws IOException {
		Object obj = null;
		ContextObjectInputStream ois = new ContextObjectInputStream(input, classLoader);
		try {
			obj = ois.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e.getMessage());
		}
		ois.close();
		return obj;
	}
}
