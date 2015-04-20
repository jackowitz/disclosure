package dcnet;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

import scheduler.ServerScheduler;
import scheduler.SlotUtils;

import scheduler.control.PruningBinaryControlSlot;
import scheduler.control.BinaryControlSlot;
import scheduler.control.DummyControlSlot;
import scheduler.control.ControlSlot;

import services.BloomFilter;

public class Server extends Base {
	public static final int CLIENT_PORT = 9495;
	public static final int SERVER_PORT = 6566;

	private int id, numClients, numServers;
	private SlotCipher cipher;
	private ServerScheduler scheduler;

	private Socket[] clientSockets;
	private Socket[] serverSockets;

	private Logger logger;

	public Server(Properties properties, int id, int numClients, int numServers) {
		super(properties);

		this.id = id;
		this.numClients = numClients;
		this.numServers = numServers;

		double[] params = BloomFilter.getParameterEstimate(estimatedElementsPerRound, fpr);
		int slotCount = (int) params[0];

		this.scheduler = new ServerScheduler(slotCount);

		this.cipher = new SlotCipher(getSecrets());
		this.logger = Logger.getGlobal();
	}

	public void initializeConnections() throws IOException {
		// Start listening for incoming connections.
		ServerSocket serverSocket;
		ServerSocket clientSocket;
		try {
			// The other servers connect to us here.
			serverSocket = new ServerSocket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(SERVER_PORT + id));

			// And clients connect to us here.
			clientSocket = new ServerSocket();
			clientSocket.setReuseAddress(true);
			clientSocket.bind(new InetSocketAddress(CLIENT_PORT + id));
		} catch (IOException e) {
			logger.severe("Exception creating listening sockets.");
			throw e;
		}

		// Initialize all server connections first:
		// 	- Connect to servers with a id greater than ours.
		serverSockets = new Socket[numServers - 1];
		for (int i = 0; i < id; i++) {
			try {
				String server = (servers == null) ? "localhost" : servers[i];
				Socket socket = new Socket(server, SERVER_PORT + i);
				serverSockets[i] = socket;
			} catch (UnknownHostException e) {
				logger.severe("Unknown server host.");
				throw e;
			}
		}
		//  - Wait for connections from those with lesser id.
		for (int i = id + 1; i < numServers; i++) {
			try {
				Socket socket = serverSocket.accept();
				serverSockets[i - 1] = socket;
			} catch (IOException e) {
				logger.severe("Exception accepting server connection.");
				throw e;
			}
		}
		{ // All servers successfully connected.
			String fmt =  "All (%d) servers connected.";
			logger.info(String.format(fmt, numServers - 1));
		}

		// For now assumes we know how many clients to expect;
		// wait for them all to connect before proceeding.
		final int connectingClients = connectingClients();
		clientSockets = new Socket[connectingClients];
		for (int i = 0; i < connectingClients; i++) {
			if ((i > 0) && (i % 5 == 0)) {
				String fmt = "%d/%d clients connected.";
				logger.info(String.format(fmt,i, connectingClients));
			}
			try {
				Socket socket = clientSocket.accept();
				clientSockets[i] = socket;
			} catch (IOException e) {
				logger.severe("Exception accepting client connection.");
				throw e;
			}
		}
		{ // All clients successfully connected.
			String fmt =  "All (%d) clients connected.";
			logger.info(String.format(fmt, connectingClients));
		}
	}

	private void startProtocolRound() throws IOException {
		ControlSlot controlSlot;
		if (controlSlotType) {
			logger.info("Running server with CONTROL_SLOTS.");
			controlSlot = new PruningBinaryControlSlot(scheduler, attemptsPerSlot);
		} else {
			controlSlot = new DummyControlSlot(scheduler, attemptsPerSlot);
		}

		long controlSlotStart = System.currentTimeMillis();

		final int controlSlotLength = controlSlot.getLength();
		if (controlSlotLength > 0) {
			final byte[] dataBuffer = new byte[controlSlotLength];
			final byte[] slotBuffer = new byte[controlSlotLength];

			SocketUtils.read(slotBuffer, dataBuffer, clientSockets);
			cipher.xorKeyStream(slotBuffer);

			SocketUtils.write(slotBuffer, serverSockets);
			SocketUtils.read(slotBuffer, dataBuffer, serverSockets);

			controlSlot.setResult(slotBuffer);
			SocketUtils.write(slotBuffer, clientSockets);
		}

		long controlSlotEnd = System.currentTimeMillis();

		final int slotCount = controlSlot.getSlotCount();
		final int attempts = controlSlot.getAttempts();

		// Record all the transmitted slots for output later.
		byte[][] slotOutputs = new byte[slotCount][];

		// Simple statistics, to make sure it's working.
		int bytes = 0;
		int collisionSlots = 0;
		int emptySlots = slotCount;

		// When the round started, used for periodic reporting.
		long first = System.currentTimeMillis();

		// Scratch space for slot data - re-used as needed.
		byte[] dataBuffer = new byte[defaultSlotLength];

		for (int i = 0; i < slotCount; i++) {
			// Periodic debug/performance statistics.
			final int sampleInterval = 10;
			if (i % sampleInterval == 0) {
				long elapsed = System.currentTimeMillis() - first;
				double rate = 1000 * ((double) i / elapsed);

				String fmt = "Slot #%d: %f slots/sec.";
				logger.log(Level.FINE, String.format(fmt, i, rate));
			}

			// Start off assuming slot is going to be empty.
			boolean slotEmpty = true;
			boolean collision = false;

			for (int j = 0; j < attempts; j++) {
				// Keeps the running total, initially XOR of secrets.
				byte[] slotBuffer = new byte[defaultSlotLength];
				cipher.xorKeyStream(slotBuffer);

				// Get ciphertexts from all connected clients.
				SocketUtils.read(slotBuffer, dataBuffer, clientSockets);

				// Send our aggregate ciphertext to the other servers.
				// Get the other servers' aggregate ciphertexts.
				SocketUtils.write(slotBuffer, serverSockets);
				SocketUtils.read(slotBuffer, dataBuffer, serverSockets);

				// slotBuffer should now contain the plaintext. Do some
				// sanity checking on it, simplistically for now, and
				// then stash it away for writing out later.
				SlotUtils.SlotMetadata meta = SlotUtils.decode(slotBuffer);
				if (meta.length > 0) {
					if (!meta.isValid) {
						logger.warning(String.format("Collision in slot %d.", i));
						collision = true;
					} else if (slotOutputs[i] == null) {
						slotOutputs[i] = slotBuffer;
						slotEmpty = false;
					}
				} else {
					for (byte b : slotBuffer) {
						if (b != 0) {
							logger.warning(String.format("Collision in slot %d.", i));
							collision = true;
							break;
						}
					}
				}
				bytes += slotBuffer.length;

				// Send the plaintext back down to the clients, if needed.
				if (true) {
					SocketUtils.write(slotBuffer, clientSockets);
				}
			}

			// Adjust the count of empty slots and collision slots.
			slotEmpty &= !collision;
			emptySlots -= !slotEmpty ? 1 : 0;
			collisionSlots += collision ? 1 : 0;
		}

		// Write the output of the round to a file for analysis.
		if (false) {
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

		{ // Dump the final round statistics.
			long elapsed = System.currentTimeMillis() - first;
			String fmt = "slots=%d (%d), bytes=%d (%d), time=%d (%d), collisions=%d, empty=%d";
			logger.info(String.format(fmt, slotCount, scheduler.getSlotCount(),
						bytes, controlSlot.getLength(), elapsed, controlSlotEnd - controlSlotStart,
						collisionSlots, emptySlots));
		}
	}

	/**
	 * Generate and return the secrets shared between this server
	 * and all clients (not just those connected).
	 * @return array of secrets, one per client, ordered by id
	 */
	private long[] getSecrets() {
		long[] secrets = new long[numClients];
		for (int i = 0; i < secrets.length; i++) {
			secrets[i] = ((long) id << 16) | i;
		}
		return secrets;
	}

	private int connectingClients() {
		int remainder = numClients % numServers;
		return (numClients / numServers) + (id < remainder ? 1 : 0);
	}

	public static void main(String[] args) {
		int id = Integer.valueOf(args[0]);
		int clients = Integer.valueOf(args[1]);
		int servers = Integer.valueOf(args[2]);

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

		Server server = new Server(properties, id, clients, servers);
		try {
			server.initializeConnections();
			server.startProtocolRound();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
