package dcnet;

import java.util.Random;

public class XORCipher {
	private static final int DEFAULT_BUFFER_SIZE = 1024;

	private static class Buffer {
		public byte[] b;
		public int u;

		public Buffer(int size) {
			b = new byte[size];
			u = 0;
		}
	}

	private Random random;
	private Buffer buffer;

	public XORCipher(long key) {
		this(key, DEFAULT_BUFFER_SIZE);
	}

	public XORCipher(long key, int blockSize) {
		random = new Random(key);
		buffer = new Buffer(blockSize);
	}

	public void xorKeyStream(byte[] src, byte[] dst) {
		for (int i = 0; i < dst.length; i++) {
			if (buffer.u >= buffer.b.length) {
				random.nextBytes(buffer.b);
				buffer.u = 0;
			}
			dst[i] = (byte) (src[i] ^ buffer.b[buffer.u]);
			buffer.u++;
		}
	}

	public static void xorBytes(byte[] src, byte[] dst) {
		int len = Math.min(src.length, dst.length);
		for (int i = 0; i < len; i++) {
			dst[i] ^= src[i];
		}
	}
}
