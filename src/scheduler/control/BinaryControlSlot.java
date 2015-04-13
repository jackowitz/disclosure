package scheduler.control;

import java.util.BitSet;
import java.util.Random;

public class BinaryControlSlot extends DummyControlSlot {

	private byte[] coinFlips;

	private BitSet successBits;
	private BitSet coinBits;

	public BinaryControlSlot(Scheduler scheduler, int attempts) throws IllegalArgumentException {
		super(scheduler, attempts);
		if (attempts % 8 != 0) {
			String msg = "attempts must be evenly divisible by 8";
			throw new IllegalArgumentException(msg);
		}

		final int slotCount = scheduler.getSlotCount();
		final int bits = attempts * slotCount;
		final int bytes = bits / 8;
		this.coinFlips = new byte[bytes];

		// Generate random bits for all slots and then zero out
		// the ones we don't actually need.
		random.nextBytes(coinFlips);

		for (int i = 0; i < coinFlips.length; i++) {
			final int slotIndex = i / (attempts / 8);
			if (scheduler.isEmpty(slotIndex)) {
				coinFlips[i] = 0;
			}
		}
	}

	@Override
	public int getLength() {
		return coinFlips.length;
	}

	@Override
	public byte[] getSlot(byte[] buffer) {
		System.arraycopy(coinFlips, 0, buffer, 0, coinFlips.length);
		return buffer;
	}

	@Override
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

	@Override
	public int getSlotCount() {
		return scheduler.getSlotCount();
	}

	@Override
	public int getAttempts() {
		return 1;
	}

	@Override
	public boolean isEmpty(int index) {
		return !coinBits.get(index) || scheduler.isEmpty(index);
	}

	@Override
	public int getLength(int index) {
		return coinBits.get(index) ? scheduler.getLength(index) : 0;
	}

	@Override
	public byte[] getSlot(int index, byte[] buffer) {
		if (coinBits.get(index)) {
			scheduler.getSlot(index, buffer);
		}
		return buffer;
	}
}
