package com.schooner.MemCached;

import javax.security.auth.callback.CallbackHandler;

/**
 * @author Meng Li
 * @since 2.6.1
 * @see AuthInfo
 */
public class AuthInfo {

	private final CallbackHandler callbackHandler;
	private final String[] mechanisms;

	public AuthInfo(CallbackHandler callbackHandler, String[] mechanisms) {
		super();
		this.callbackHandler = callbackHandler;
		this.mechanisms = mechanisms;
	}

	public static AuthInfo plain(String username, String password) {
		return new AuthInfo(new PlainCallbackHandler(username, password), new String[] { "PLAIN" });
	}

	public static AuthInfo cramMD5(String username, String password) {
		return new AuthInfo(new PlainCallbackHandler(username, password), new String[] { "CRAM-MD5" });
	}

	public static AuthInfo typical(String username, String password) {
		return new AuthInfo(new PlainCallbackHandler(username, password), new String[] { "CRAM-MD5", "PLAIN" });
	}

	public CallbackHandler getCallbackHandler() {
		return callbackHandler;
	}

	public String[] getMechanisms() {
		return mechanisms;
	}

}
