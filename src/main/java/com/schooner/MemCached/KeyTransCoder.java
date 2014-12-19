package com.schooner.MemCached;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KeyTransCoder} is the default object translator,it used to change the
 * key to String to
 *
 * @author xionghui
 *
 */
public class KeyTransCoder implements TransBytecode {
	// logger
	public static final Logger LOG = LoggerFactory
			.getLogger(KeyTransCoder.class);

	@Override
	public String encode(Serializable origin) {
		String result = null;
		if (origin instanceof String) {
			result = Constant.STRING_PREFIX + (String) origin;
		} else if (origin instanceof StringBuilder
				|| origin instanceof StringBuffer) {
			result = Constant.STRING_PREFIX + origin;
		} else if (origin instanceof Boolean) {
			result = Constant.BOOLEAN_PREFIX
					+ Boolean.toString((Boolean) origin);
		} else if (origin instanceof Byte) {
			result = Constant.BYTE_PREFIX + Byte.toString((Byte) origin);
		} else if (origin instanceof Short) {
			result = Constant.SHORT_PREFIX + Short.toString((Short) origin);
		} else if (origin instanceof Character) {
			result = Constant.CHARACTER_PREFIX
					+ Character.toString((Character) origin);
		} else if (origin instanceof Integer) {
			result = Constant.INTEGER_PREFIX
					+ Integer.toString((Integer) origin);
		} else if (origin instanceof Long) {
			result = Constant.LONG_PREFIX + Long.toString((Long) origin);
		} else if (origin instanceof Float) {
			result = Constant.FLOAT_PREFIX + Float.toString((Float) origin);
		} else if (origin instanceof Double) {
			result = Constant.DOUBLE_PREFIX + Double.toString((Double) origin);
		} else if (origin instanceof Date) {
			result = Constant.DATE_PREFIX + ((Date) origin).getTime();
		} else {
			byte[] bys = serializable(origin);
			if (origin.getClass().isArray()) {
				result = Constant.ARRAY_PREFIX + byte2hex(bys);
			} else {
				result = Constant.OBJECT_PREFIX + byte2hex(bys);
			}
		}
		return result;
	}

	/**
	 * Get Object's byte code
	 *
	 * @param key
	 * @return
	 */
	private byte[] serializable(Serializable key) {
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream(256);
		try {
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(
					byteStream);
			objectOutputStream.writeObject(key);
			objectOutputStream.flush();
		} catch (IOException e) {
			// exception thrown
			if (LOG.isErrorEnabled()) {
				LOG.error("++++ exception thrown while serializabling");
				LOG.error(e.getMessage(), e);
			}
		}
		return byteStream.toByteArray();
	}

	/**
	 * Change byte code to String
	 */
	private String byte2hex(byte[] bys) {
		StringBuilder builder = new StringBuilder();
		String tmp = null;
		for (int i = 0, len = bys.length; i < len; i++) {
			// exchange integer to hex
			tmp = (java.lang.Integer.toHexString(bys[i] & 0XFF));
			if (tmp.length() == 1) {
				builder.append("0");
			}
			builder.append(tmp);
		}
		return builder.toString();
	}

	/**
	 * Prefix of java class.
	 *
	 * @author xionghui
	 */
	private static class Constant {
		private static final String STRING_PREFIX = "S_";
		private static final String BOOLEAN_PREFIX = "B_";
		private static final String INTEGER_PREFIX = "I_";
		private static final String LONG_PREFIX = "L_";
		private static final String CHARACTER_PREFIX = "C_";
		private static final String FLOAT_PREFIX = "F_";
		private static final String SHORT_PREFIX = "SH_";
		private static final String DOUBLE_PREFIX = "D_";
		private static final String DATE_PREFIX = "DATE_";
		private static final String BYTE_PREFIX = "BYTE_";

		private static final String ARRAY_PREFIX = "A_";
		private static final String OBJECT_PREFIX = "O_";
	}
}
