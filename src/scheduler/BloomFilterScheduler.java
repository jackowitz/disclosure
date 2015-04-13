package scheduler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import services.MultiHash;

public class BloomFilterScheduler {

	private MultiHash hash;
	private byte[][] slots;
	private int[] slotCounts;

	private int elementCount;
	private int filledCount;

	public BloomFilterScheduler(int m, int k) {
		hash = new MultiHash(k, m);
		slots = new byte[m][];
		slotCounts = new int[m];
	}

	public boolean add(String value) {
		try {
			byte[] bytes = value.getBytes("ISO-8859-1");
			return add(bytes);
		} catch (UnsupportedEncodingException e) {
			return false;
		}
	}

	public boolean add(byte[] value) {
		int[] indices = hash.hash(value);
		for (int index : indices) {
			slotCounts[index]++;
			if (slotCounts[index] == 1) {
				slots[index] = value;
				filledCount++;
			} else {
				slots[index] = null;
				filledCount--;
			}
		}
		elementCount++;
		return true;
	}

	public int getSlotCount() {
		return slots.length;
	}

	public int getFilledCount() {
		return filledCount;
	}

	/**
	 * Encode the index-th slot into the provided byte array.
	 * The first 12 bytes are reserved for encoding metadata
	 * about the slot; 4 bytes for the length, 8 for the
	 * CRC32 checksum of the data.
	 */
	public byte[] getSlot(int index, byte[] buffer) {
		byte[] slot = slots[index];
		if (slot != null) {
			final int offset = SlotUtils.METADATA_BYTES;
			System.arraycopy(slot, 0, buffer, offset, slot.length);
			SlotUtils.encode(buffer, slot.length);
		}
		return buffer;
	}

	public boolean isEmpty(int index) {
		return slots[index] == null;
	}

	public int getLength(int index) {
		final byte[] slot = slots[index];
		return (slot == null) ? 0 : slot.length;
	}

	public void writeSlotsToFile(String outputFile) throws IOException {
		writeSlotsToFile(outputFile, false);
	}

	public void writeSlotsToFile(String outputFile, boolean verbose) throws IOException {
		try (
			FileWriter fw = new FileWriter(outputFile);
			BufferedWriter bw = new BufferedWriter(fw);
		) {
			for (int i = 0; i < slots.length; i++) {
				if (slots[i] != null) {
					String slotValue = new String(slots[i], "ISO-8859-1"); 
					bw.write(String.format("%04d: ", i));
					bw.write(slotValue);
					bw.newLine();
				} else if (verbose) {
					bw.write(String.format("%04d: <EMPTY>\n", i));
				}
			}
		}
	}
}
