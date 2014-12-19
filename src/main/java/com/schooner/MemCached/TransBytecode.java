package com.schooner.MemCached;

import java.io.Serializable;

/**
 * {@link TransBytecode} is used to translate the {@link Serializable} Object to
 * String.
 *
 * @author xionghui
 *
 */
public interface TransBytecode {
	public String encode(Serializable origin);
}
