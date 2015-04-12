package scheduler;

import java.util.zip.CRC32;
import java.nio.ByteBuffer;
import java.io.UnsupportedEncodingException;

public class SlotUtils {
	private static final int LENGTH_BYTES = 4;
	private static final int CHECKSUM_BYTES = 8;

	public static final int METADATA_BYTES = LENGTH_BYTES + CHECKSUM_BYTES;

	/**
	 * Encodes a slot with relevant metadata. Note that this
	 * method overwrites the first METADATA_BYTE bytes of the
	 * array.
	 */
	public static void encode(byte[] buffer, int length) {
		final int offset = METADATA_BYTES;

		// Write the length to first 4 bytes of buffer.
		ByteBuffer wrapper = ByteBuffer.wrap(buffer, 0, offset);
		wrapper.putInt(length);

		// Update checksum with full slot contents.
		CRC32 crc32 = new CRC32();
		crc32.update(buffer, offset, buffer.length - offset);
		wrapper.putLong(crc32.getValue());
	}

	public static class SlotMetadata {
		public int offset = METADATA_BYTES;
		public int length;

		public boolean isValid;
	}

	public static SlotMetadata decode(byte[] buffer) {
		final int offset = METADATA_BYTES;
		SlotMetadata meta = new SlotMetadata();

		// Get the length and checksum from the slot metadata.
		ByteBuffer wrapper = ByteBuffer.wrap(buffer, 0, offset);
		final int length = wrapper.getInt();
		final long checksum = wrapper.getLong();

		// Don't bother with checksum on empty slot.
		if (length > 0) {
			// Update checksum with full slot contents.
			CRC32 crc32 = new CRC32();
			crc32.update(buffer, offset, buffer.length - offset);
			meta.isValid = (crc32.getValue() == checksum);
		}
		meta.length = length;
		return meta;
	}

	public static String toString(byte[] buffer) throws UnsupportedEncodingException {
		final int offset = METADATA_BYTES;

		// Get the length from the slot metadata.
		ByteBuffer wrapper = ByteBuffer.wrap(buffer, 0, offset);
		int length = wrapper.getInt();

		// Decode slot data as String and write out.
		return new String(buffer, offset, length, "ISO-8859-1"); 
	}
}
