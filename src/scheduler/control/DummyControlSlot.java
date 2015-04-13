package scheduler.control;

import java.util.Random;

public class DummyControlSlot implements ControlSlot {

	protected Scheduler scheduler;
	protected int attempts;

	protected Random random;

	public DummyControlSlot(Scheduler scheduler, int attempts) {
		this.scheduler = scheduler;
		this.attempts = attempts;

		this.random = new Random();
	}

	public int getLength() {
		return 0;
	}

	public byte[] getSlot(byte[] buffer) {
		return buffer;
	}

	public void setResult(byte[] buffer) { }

	public int getSlotCount() {
		return scheduler.getSlotCount();
	}

	public int getAttempts() {
		return attempts;
	}

	public boolean isEmpty(int index) {
		return scheduler.isEmpty(index);
	}

	public int getLength(int index) {
		return scheduler.getLength(index);
	}

	public byte[] getSlot(int index, byte[] buffer) {
		if (!isEmpty(index) && random.nextBoolean())
			scheduler.getSlot(index, buffer);
		return buffer;
	}
}
