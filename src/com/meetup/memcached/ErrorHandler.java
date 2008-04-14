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
 * in the event of an error in {@link MemcachedClient}. The implementor can do
 * things like flush caches or perform additioonal logging.
 *
 * @author Dan Zivkovic <zivkovic@apple.com>
 */
package com.meetup.memcached;

public interface ErrorHandler {

    /**
     * Called for errors thrown during initialization.
     */
    public void handleErrorOnInit( final MemcachedClient client ,
                                   final Throwable error );

    /**
     * Called for errors thrown during {@link MemcachedClient#get(String)} and related methods.
     */
    public void handleErrorOnGet( final MemcachedClient client ,
                                  final Throwable error ,
                                  final String cacheKey );

    /**
     * Called for errors thrown during {@link MemcachedClient#getMulti(String)} and related methods.
     */
    public void handleErrorOnGet( final MemcachedClient client ,
                                  final Throwable error ,
                                  final String[] cacheKeys );

    /**
     * Called for errors thrown during {@link MemcachedClient#set(String,Object)} and related methods.
     */
    public void handleErrorOnSet( final MemcachedClient client ,
                                  final Throwable error ,
                                  final String cacheKey );

    /**
     * Called for errors thrown during {@link MemcachedClient#delete(String)} and related methods.
     */
    public void handleErrorOnDelete( final MemcachedClient client ,
                                     final Throwable error ,
                                     final String cacheKey );

    /**
     * Called for errors thrown during {@link MemcachedClient#flushAll()} and related methods.
     */
    public void handleErrorOnFlush( final MemcachedClient client ,
                                    final Throwable error );

    /**
     * Called for errors thrown during {@link MemcachedClient#stats()} and related methods.
     */
    public void handleErrorOnStats( final MemcachedClient client ,
                                    final Throwable error );

} // interface
