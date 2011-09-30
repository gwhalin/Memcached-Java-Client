/**
 * Copyright (c) 2008 Greg Whalin
 * All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the BSD license
 *
 * This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.
 *
 * You should have received a copy of the BSD License along with this
 * library.
 *
 * This is an interface implemented by classes that want to receive callbacks
 * in the event of an error in {@link MemCachedClient}. The implementor can do
 * things like flush caches or perform additioonal logging.
 *
 * @author Dan Zivkovic <zivkovic@apple.com>
 */

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
package com.danga.MemCached;

/**
 * You can customize your error handle processes in this class.
 * 
 */
public interface ErrorHandler {

	/**
	 * Called for errors thrown during initialization.
	 */
	public void handleErrorOnInit(final MemCachedClient client, final Throwable error);

	/**
	 * Called for errors thrown during {@link MemCachedClient#get(String)} and
	 * related methods.
	 */
	public void handleErrorOnGet(final MemCachedClient client, final Throwable error, final String cacheKey);

	/**
	 * Called for errors thrown during {@link MemCachedClient#getMulti(String)}
	 * and related methods.
	 */
	public void handleErrorOnGet(final MemCachedClient client, final Throwable error, final String[] cacheKeys);

	/**
	 * Called for errors thrown during
	 * {@link MemCachedClient#set(String,Object)} and related methods.
	 */
	public void handleErrorOnSet(final MemCachedClient client, final Throwable error, final String cacheKey);

	/**
	 * Called for errors thrown during {@link MemCachedClient#delete(String)}
	 * and related methods.
	 */
	public void handleErrorOnDelete(final MemCachedClient client, final Throwable error, final String cacheKey);

	/**
	 * Called for errors thrown during {@link MemCachedClient#flushAll()} and
	 * related methods.
	 */
	public void handleErrorOnFlush(final MemCachedClient client, final Throwable error);

	/**
	 * Called for errors thrown during {@link MemCachedClient#stats()} and
	 * related methods.
	 */
	public void handleErrorOnStats(final MemCachedClient client, final Throwable error);

} // interface
