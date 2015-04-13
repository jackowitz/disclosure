package scheduler.control;

import java.util.BitSet;
import java.util.Random;

public class ControlSlot {

	/**
	 * We can build a control slot for any scheduler that can
	 * provide the following information about the slots.
	 */
	public interface Scheduler {
		public int getSlotCount();

		public boolean isEmpty(int index);
		public int getLength(int index);

		public byte[] getSlot(int index, byte[] buffer);
	}

	private Scheduler scheduler;
	private int attempts;

	private byte[] coinFlips;
	private Random random;

	private BitSet successBits;
	private BitSet coinBits;

	public ControlSlot(Scheduler scheduler, int attempts) throws IllegalArgumentException {
		if (attempts % 8 != 0) {
			String msg = "attempts must be evenly divisible by 8";
			throw new IllegalArgumentException(msg);
		}
		this.scheduler = scheduler;
		this.attempts = attempts;

		final int slotCount = scheduler.getSlotCount();
		final int bits = attempts * slotCount;
		final int bytes = bits / 8;
		this.coinFlips = new byte[bytes];

		// Generate random bits for all slots and then zero out
		// the ones we don't actually need.
		this.random = new Random();
		random.nextBytes(coinFlips);

		for (int i = 0; i < coinFlips.length; i++) {
			final int slotIndex = i / (attempts / 8);
			if (scheduler.isEmpty(slotIndex)) {
				coinFlips[i] = 0;
			}
		}
	}

	public int getLength() {
		return coinFlips.length;
	}

	public byte[] getSlot(byte[] buffer) {
		System.arraycopy(coinFlips, 0, buffer, 0, coinFlips.length);
		return buffer;
	}

	public void setResult(byte[] result) {
		// valueOf makes its own copy of the array so its
		// safe to just pass it in here.
		final BitSet resultBits = BitSet.valueOf(result);
		final BitSet flipBits = BitSet.valueOf(coinFlips);

		// Keep track of whether we were successful at all within
		// the slot and, if so, which of our coin flips led to it.
		// - Heuristic: Choose the first successful attempt.
		final int slotCount = scheduler.getSlotCount();
		this.successBits = new BitSet(slotCount);
		this.coinBits = new BitSet(slotCount);

		// Go through and find the first bit set for each slot.
		// Check our coin flip for the corresponding attempt and
		// stash that away for later.
		for (int i = 0; i < slotCount; i++) {
			for (int j = 0; j < attempts; j++) {
				final int n = i * attempts + j;
				if (resultBits.get(n)) {
					successBits.set(i);
					coinBits.set(i, flipBits.get(n));
					break;
				}
			}
		}
	}

	public int getSlotCount() {
		return scheduler.getSlotCount();
	}

	public int getAttempts() {
		return 1;
	}

	public boolean isEmpty(int index) {
		return !coinBits.get(index) || scheduler.isEmpty(index);
	}

	public int getLength(int index) {
		return coinBits.get(index) ? scheduler.getLength(index) : 0;
	}

	public byte[] getSlot(int index, byte[] buffer) {
		if (coinBits.get(index)) {
			scheduler.getSlot(index, buffer);
		}
		return buffer;
	}
}
