package scheduler;

import scheduler.control.ControlSlot;

public class ServerScheduler implements ControlSlot.Scheduler {

	private int slotCount;

	public ServerScheduler(int slotCount) {
		this.slotCount = slotCount;
	}

	public int getSlotCount() {
		return slotCount;
	}

	public boolean isEmpty(int index) {
		return true;
	}

	public int getLength(int index) {
		return 0;
	}

	public byte[] getSlot(int index, byte[] buffer) {
		return buffer;
	}
}
