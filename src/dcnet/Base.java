package dcnet;

import java.util.Properties;

public abstract class Base {
	public static final String PROP_SLOT_ELEMENTS = "slot.estimatedElementsPerRound";
	public static final String PROP_SLOT_FPR = "slot.fpr";

	public static final String PROP_SLOT_ATTEMPTS = "slot.attemptsPerSlot";
	public static final String PROP_SLOT_CONTROL = "slot.controlSlotType";

	public static final String PROP_SLOT_LENGTH = "slot.defaultLength";

	public static final String PROP_DCNET_SERVERS = "dcnet.servers";

	protected Properties properties;

	protected int estimatedElementsPerRound;
	protected double fpr;

	protected int attemptsPerSlot;
	protected boolean controlSlotType;

	protected int defaultSlotLength;

	protected String[] servers;

	public Base(Properties properties) {
		this.properties = properties;

		this.estimatedElementsPerRound = Integer.valueOf(properties.getProperty(PROP_SLOT_ELEMENTS, "32"));
		this.fpr = Double.valueOf(properties.getProperty(PROP_SLOT_FPR, "0.05"));

		this.attemptsPerSlot = Integer.valueOf(properties.getProperty(PROP_SLOT_ATTEMPTS, "8"));
		this.controlSlotType = Boolean.valueOf(properties.getProperty(PROP_SLOT_CONTROL, "false"));

		this.defaultSlotLength = Integer.valueOf(properties.getProperty(PROP_SLOT_LENGTH, "512"));
		
		try {
			this.servers = properties.getProperty(PROP_DCNET_SERVERS).split(",");
		} catch (NullPointerException e) {
			// leave servers as null
		}
	}
}
