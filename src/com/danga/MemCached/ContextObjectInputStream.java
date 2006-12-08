/**
 * MemCached Java client
 * Copyright (c) 2007
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later
 * version.
 *
 * This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 *
 * Adds the ability for the MemCached client to be initialized
 * with a custom class loader.  This will allow for the
 * deserialization of classes that are not visible to the system
 * class loader.
 * 
 * @author Vin Chawla <vin@tivo.com> 
 * @version 1.5
 */
package com.danga.MemCached;

import java.util.*;
import java.util.zip.*;
import java.io.*;

public class ContextObjectInputStream extends ObjectInputStream { 

    ClassLoader mLoader;
    
    public ContextObjectInputStream( InputStream in, ClassLoader loader ) throws IOException, SecurityException {
        super( in );
        mLoader = loader;
    }
    
    protected Class resolveClass( ObjectStreamClass v ) throws IOException, ClassNotFoundException {
        if ( mLoader == null )
            return super.resolveClass( v );
		else
            return mLoader.loadClass( v.getName() );
    }
}
