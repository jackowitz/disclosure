package scheduler.control;

import java.util.BitSet;

public class PruningBinaryControlSlot extends BinaryControlSlot {

	private int slotCount;
	private int[] indexMap;

	public PruningBinaryControlSlot(Scheduler scheduler, int attempts) throws IllegalArgumentException {
		super(scheduler, attempts);
	}

	@Override
	public void setResult(byte[] result) {
		super.setResult(result);

		this.slotCount = successBits.cardinality();
		indexMap = new int[slotCount];

		int converted = -1;
		for (int i = 0; i < slotCount; i++) {
			converted = successBits.nextSetBit(converted + 1);
			indexMap[i] = converted;
		}
	}

	@Override
	public int getSlotCount() {
		return slotCount;
	}

	@Override
	public boolean isEmpty(int index) {
		assert(!super.isEmpty(convertIndex(index)));
		return false;
	}

	@Override
	public int getLength(int index) {
		return super.getLength(convertIndex(index));
	}

	@Override
	public byte[] getSlot(int index, byte[] buffer) {
		return super.getSlot(convertIndex(index), buffer);
	}

	private int convertIndex(int index) {
		return indexMap[index];
	}
}
