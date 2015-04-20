package scheduler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.CRC32;

import scheduler.control.ControlSlot;

import services.BloomFilter;

public class BloomFilterScheduler implements ControlSlot.Scheduler {

	private BloomFilter bloomFilter;
	private byte[][] cache;
	private byte[][] slots;

	private int elementCount;
	private int filledCount;

	public BloomFilterScheduler(int elements, double fpr) {
		double[] params = BloomFilter.getParameterEstimate(elements, fpr);
		int m = (int) params[0], k = (int) params[1];

		this.bloomFilter = new BloomFilter(k, m, true);
		this.cache = new byte[m][];
		this.slots = new byte[m][];
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
		cache[elementCount++] = value;
		return bloomFilter.insert(value);
	}

	public boolean finalizeSchedule() {
		return finalizeSchedule(Integer.MAX_VALUE);
	}

	public boolean finalizeSchedule(int slotsPerElement) {
		if (slotsPerElement < 1) {
			slotsPerElement = Integer.MAX_VALUE;
		}
		Random random = new Random();
		filledCount = 0;

		boolean collision = false;
		for (int i = 0; i < elementCount; i++) {
			int[] indices = bloomFilter.getUniqueIndices(cache[i]);
			if (indices.length < 1) {
				collision = true;
				continue;
			}

			int limit = Math.min(slotsPerElement, indices.length);

			// Pick a random sample of up to slotsPerElement indices.
			for (int j = limit + 1; j < indices.length; j++) {
				int k = random.nextInt(j);
				if (k < limit) {
					indices[k] = indices[j];
				}
			}
			// Assign the element to the slots we just picked.
			for (int j = 0; j < limit; j++) {
				slots[indices[j]] = cache[i];
				filledCount++;
			}
		}
		return collision;
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
	 * The slot data is copied into buffer so that changes to
	 * buffer do not effect the scheduler's data.
	 * @param index index of slot
	 * @param buffer copy destination
	 * @return the passed buffer (for convenience)
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
