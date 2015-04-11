package dcnet;

public class SlotCipher {
	private XORCipher[] ciphers;

	public SlotCipher(long[] secrets) {
		ciphers = new XORCipher[secrets.length];
		for (int i = 0; i < ciphers.length; i++) {
			ciphers[i] = new XORCipher(secrets[i]);
		}
	}

	public byte[] xorKeyStream(byte[] message) {
		for (XORCipher cipher : ciphers) {
			cipher.xorKeyStream(message, message);
		}
		return message;
	}
}
