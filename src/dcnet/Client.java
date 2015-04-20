package dcnet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Properties;
import java.util.Random;

import scheduler.BloomFilterScheduler;
import scheduler.SlotUtils;

import scheduler.control.PruningBinaryControlSlot;
import scheduler.control.BinaryControlSlot;
import scheduler.control.DummyControlSlot;
import scheduler.control.ControlSlot;

import services.BloomFilter;

public class Client extends Base {
	private static final String PROP_SLOT_PER_ELEMENT = "slot.slotsPerElement";

	private int id, numServers;

	private Socket serverSocket;

	private SlotCipher cipher;
	private BloomFilterScheduler scheduler;

	private Logger logger;
	private Random slotRandom;

	private int slotsPerElement;

	public Client(Properties properties, int id, int numServers) {
		super(properties);

		this.id = id;
		this.numServers = numServers;
		this.slotRandom = new Random();
		this.logger = Logger.getGlobal();

		this.cipher = new SlotCipher(getSecrets());

		slotsPerElement = Integer.valueOf(properties.getProperty(
					PROP_SLOT_PER_ELEMENT, Integer.toString(Integer.MAX_VALUE)));
		this.scheduler = new BloomFilterScheduler(estimatedElementsPerRound, fpr);

	}

	private void initializeConnection() throws IOException {
		String serverHost = (servers == null) ? "localhost" : servers[getServer()];
		int serverPort = Server.CLIENT_PORT + getServer();
		try {
			serverSocket = new Socket(serverHost, serverPort);
		} catch (IOException e) {
			logger.severe("Exception connecting to server.");
			throw e;
		}
	}

	/**
	 * Generate the secrets this client shares with the servers.
	 * For now, the test secrets aren't exactly secret; they're
	 * derived from the public ids.
	 * @return the shared secrets
	 */
	private long[] getSecrets() {
		long[] secrets = new long[numServers];
		for (int i = 0; i < secrets.length; i++) {
			secrets[i] = ((long) i << 16) | id;
		}
		return secrets;
	}

	/**
	 * Get the ID of the server this client should connect to.
	 * @return the server id
	 */
	private int getServer() {
		return id % numServers;
	}

	public void readInputFromFile(String inputFile) throws IOException {
		try (
			FileReader fr = new FileReader(inputFile);
			BufferedReader br = new BufferedReader(fr);
		) {
			String elem = null;
			while ((elem = br.readLine()) != null) {
				scheduler.add(elem);
			}
		} catch (IOException e) {
			throw e;
		}
	}

	public void finalizeSchedule() {
		scheduler.finalizeSchedule(slotsPerElement);
	}

	public void startProtocolRound() throws IOException {
		if (false) {
			scheduler.writeSlotsToFile(String.format("run/slots/%d.csv", id));
		}

		ControlSlot controlSlot;
	   	if (controlSlotType) {
			logger.info("Running client with CONTROL_SLOTS.");
			controlSlot = new PruningBinaryControlSlot(scheduler, attemptsPerSlot);
		} else {
			controlSlot = new DummyControlSlot(scheduler, attemptsPerSlot);
		}
		final int controlSlotLength = controlSlot.getLength();
		if (controlSlotLength > 0) {
			// Run control slot as one big slot for now.
			final byte[] slotBuffer = new byte[controlSlotLength];
			controlSlot.getSlot(slotBuffer);
			cipher.xorKeyStream(slotBuffer);

			// Run control slots up through the servers.
			SocketUtils.write(slotBuffer, serverSocket);
			SocketUtils.read(null, slotBuffer, serverSocket);

			controlSlot.setResult(slotBuffer);
		}

		final int slotCount = controlSlot.getSlotCount();
		final int attempts = controlSlot.getAttempts();

		// Record all the transmitted slots for output later.
		byte[][] slotOutputs = new byte[slotCount][];

		for (int i = 0; i < slotCount; i++) {
			// Start off assuming slot is going to be empty.
			boolean slotEmpty = true;

			for (int j = 0; j < attempts; j++) {
				final byte[] slotBuffer = new byte[defaultSlotLength];
				controlSlot.getSlot(i, slotBuffer);
				cipher.xorKeyStream(slotBuffer);

				SocketUtils.write(slotBuffer, serverSocket);
				if (true) {
					SocketUtils.read(null, slotBuffer, serverSocket);

					SlotUtils.SlotMetadata meta = SlotUtils.decode(slotBuffer);
					if (!meta.isEmpty) {
						if(meta.isValid && slotEmpty) {
							slotOutputs[i] = slotBuffer;
						}
						slotEmpty = false;
					}
				}
			}
		}

		// Write the output of the round to a file for analysis.
		if (true) {
			String outputFile = String.format("run/output/%d.csv", id);
			try (
				FileWriter fw = new FileWriter(outputFile);
				BufferedWriter bw = new BufferedWriter(fw);
			) {
				for (byte[] slot : slotOutputs) {
					if (slot != null) {
						bw.write(SlotUtils.toString(slot));
						bw.newLine();
					}
				}
			} catch (IOException e) {
				logger.warning("Error writing output to file.");
				throw e;
			}
		}
	}

	public static void main(String[] args) {
		int id = Integer.valueOf(args[0]);
		int servers = Integer.valueOf(args[1]);

		Properties properties = new Properties();
		try {
			FileInputStream fis = new FileInputStream("run/config.properties");
			properties.load(fis);
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Logger.getGlobal().setLevel((id == 0) ? Level.FINE : Level.INFO);

		Client client = new Client(properties, id, servers);
		try {
			String inputFile = String.format("run/input/%d.csv", id);
			client.readInputFromFile(inputFile);
			client.finalizeSchedule();

			client.initializeConnection();
			client.startProtocolRound();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
