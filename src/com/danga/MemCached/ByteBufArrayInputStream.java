/**
 * src/com/danga/MemCached/ByteBufArrayInputStream.java
 *
 * @author $Author: $
 * @version $Revision: $ $Date: $
 * copyright (c) 2006 meetup, inc
 *
 * $Id: $
 */
package com.danga.MemCached;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class ByteBufArrayInputStream extends InputStream implements LineInputStream {
	private ByteBuffer[] bufs;
	private int currentBuf = 0;
	
	public ByteBufArrayInputStream( List<ByteBuffer> bufs ) throws Exception {
		this( bufs.toArray( new ByteBuffer[] {} ) );
	}
	
	public ByteBufArrayInputStream( ByteBuffer[] bufs ) throws Exception {
		if ( bufs == null || bufs.length == 0 )
			throw new Exception( "buffer is empty" );
		
		this.bufs = bufs;
		for ( ByteBuffer b : bufs )
			b.flip();
	}
	
	public int read() {
		do {
			if ( bufs[currentBuf].hasRemaining() )
				return bufs[currentBuf].get();
			currentBuf++;
		}
		while ( currentBuf < bufs.length );
		
		currentBuf--;
		return -1;
	}
	
	public int read( byte[] buf ) {
		int len = buf.length;
		int bufPos = 0;
		do {
			if ( bufs[currentBuf].hasRemaining() ) {
				int n = Math.min( bufs[currentBuf].remaining(), len-bufPos );
				bufs[currentBuf].get( buf, bufPos, n );
				bufPos += n;
			}
			currentBuf++;
		}
		while ( currentBuf < bufs.length && bufPos < len );
		
		currentBuf--;
		
		if ( bufPos > 0 || ( bufPos == 0 && len == 0 ) )
			return bufPos;
		else
			return -1;
	}
	
	public String readLine() throws IOException {
		byte[] b = new byte[1];
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		boolean eol = false;
		
		while ( read( b, 0, 1 ) != -1 ) {
			if ( b[0] == 13 ) {
				eol = true;
			}
			else {
				if ( eol ) {
					if ( b[0] == 10 )
						break;
					eol = false;
				}
			}
			
			// cast byte into char array
			bos.write( b, 0, 1 );
		}
		
		if ( bos == null || bos.size() <= 0 ) {
			throw new IOException( "++++ Stream appears to be dead, so closing it down" );
		}
		
		// else return the string
		return bos.toString().trim();
	}
	
	public void clearEOL() throws IOException {
		byte[] b = new byte[1];
		boolean eol = false;
		while ( read( b, 0, 1 ) != -1 ) {
		
			// only stop when we see
			// \r (13) followed by \n (10)
			if ( b[0] == 13 ) {
				eol = true;
				continue;
			}
			
			if ( eol ) {
				if ( b[0] == 10 )
					break;
				eol = false;
			}
		}
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder( "ByteBufArrayIS: " );
		sb.append( bufs.length ).append( " bufs of sizes: \n" );

		for ( int i=0; i < bufs.length; i++ ) {
			sb.append( "                                        " )
				.append (i ).append( ":  " ).append( bufs[i] ).append( "\n" );
		}
		return sb.toString();
	}
}
