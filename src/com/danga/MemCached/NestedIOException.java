/**
 * MemCached Java client
 * Copyright (c) 2007 Greg Whalin
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
 * @author Kevin A. Burton <burton@peerfear.org> 
 * @version 2.0
 */
package com.danga.MemCached;

import java.io.*;

/**
 * Bridge class to provide nested Exceptions with IOException which has
 * constructors that don't take Throwables.
 * 
 * @author <a href="mailto:burton@rojo.com">Kevin Burton</a>
 * @version 1.2
 */
public class NestedIOException extends IOException {

    /**
     * Create a new <code>NestedIOException</code> instance.
     * @param cause object of type throwable
     */
    public NestedIOException( Throwable cause ) {
        super( cause.getMessage() );
        super.initCause( cause );
    }

    public NestedIOException( String message, Throwable cause ) {
        super( message );
        initCause( cause );
    }
}
