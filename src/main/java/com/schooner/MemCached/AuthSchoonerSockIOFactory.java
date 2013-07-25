package com.schooner.MemCached;

import java.io.DataInputStream;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.whalin.MemCached.MemCachedClient;

/**
 * * {@link AuthSchoonerSockIOFactory} is used to create and destroy socket for
 * connection pool with authorized information.
 * 
 * @author Meng Li
 * @since 2.6.1
 * @see AuthSchoonerSockIOFactory
 */
public class AuthSchoonerSockIOFactory extends SchoonerSockIOFactory {

	// logger
	public static Logger log = LoggerFactory.getLogger(AuthSchoonerSockIOFactory.class);

	public final static String NTLM = "NTLM";
	public final static String PLAIN = "PLAIN";
	public final static String LOGIN = "LOGIN";
	public final static String DIGEST_MD5 = "DIGEST-MD5";
	public final static String CRAM_MD5 = "CRAM-MD5";
	public final static String ANONYMOUS = "ANONYMOUS";

	public static final byte[] EMPTY_BYTES = new byte[0];

	private AuthInfo authInfo;

	public AuthSchoonerSockIOFactory(String host, boolean isTcp, int bufferSize, int socketTO, int socketConnectTO,
			boolean nagle, AuthInfo authInfo) {
		super(host, isTcp, bufferSize, socketTO, socketConnectTO, nagle);
		this.authInfo = authInfo;
	}

	@Override
	public Object makeObject() throws Exception {
		SchoonerSockIO socket = createSocket(host);
		auth(socket);
		return socket;
	}

	private void auth(SchoonerSockIO socket) throws Exception {
		SaslClient saslClient = Sasl.createSaslClient(authInfo.getMechanisms(), null, "memcached", host, null,
				this.authInfo.getCallbackHandler());

		byte[] authData = saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(EMPTY_BYTES) : EMPTY_BYTES;

		authData = sendAuthData(socket, MemCachedClient.OPCODE_START_AUTH, saslClient.getMechanismName(), authData);
		if (authData == null)
			return;
		authData = saslClient.evaluateChallenge(authData);
		if (sendAuthData(socket, MemCachedClient.OPCODE_AUTH_STEPS, saslClient.getMechanismName(), authData) == null)
			return;

		if (log.isErrorEnabled())
			log.error("Auth Failed: mechanism = " + saslClient.getMechanismName());
		throw new Exception();
	}

	private byte[] sendAuthData(SchoonerSockIO sock, byte opcode, String mechanism, byte[] authData) throws Exception {
		sock.writeBuf.clear();
		sock.writeBuf.put(MemCachedClient.MAGIC_REQ);
		sock.writeBuf.put(opcode);
		sock.writeBuf.putShort((short) mechanism.length());
		sock.writeBuf.putInt(0);
		sock.writeBuf.putInt(mechanism.length() + authData.length);
		sock.writeBuf.putInt(0);
		sock.writeBuf.putLong(0);
		sock.writeBuf.put(mechanism.getBytes());
		sock.writeBuf.put(authData);

		// write the buffer to server
		// now write the data to the cache server
		sock.flush();
		// get result code
		DataInputStream dis = new DataInputStream(new SockInputStream(sock, Integer.MAX_VALUE));
		dis.readInt();
		dis.readByte();
		dis.readByte();
		byte[] response = null;
		short status = dis.readShort();
		if (status == MemCachedClient.FURTHER_AUTH) {
			int length = dis.readInt();
			response = new byte[length];
			dis.readInt();
			dis.readLong();
			dis.read(response);
		} else if (status == MemCachedClient.AUTH_FAILED) {
			if (log.isErrorEnabled())
				log.error("Auth Failed: mechanism = " + mechanism);
			dis.close();
			throw new Exception();
		}
		dis.close();
		return response;
	}
}
