package scheduler.control;

public interface ControlSlot {

	/**
	 * We can build a control slot from any scheduler that can
	 * provide the following information about the slots.
	 */
	public interface Scheduler {
		public int getSlotCount();

		public boolean isEmpty(int index);
		public int getLength(int index);

		public byte[] getSlot(int index, byte[] buffer);
	}

	public int getLength();
	public byte[] getSlot(byte[] buffer);

	public void setResult(byte[] result);

	public int getSlotCount();
	public int getAttempts();

	public boolean isEmpty(int index);
	public int getLength(int index);

	public byte[] getSlot(int index, byte[] buffer);
}
