package dcnet;

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
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

import services.BloomFilter;

public class Server {
	public static final int CLIENT_PORT = 9495;
	public static final int SERVER_PORT = 6566;

	private int id, numClients, numServers;
	private SlotCipher cipher;
	private int slotCount;

	private Socket[] clientSockets;
	private Socket[] serverSockets;

	private Logger logger;

	public Server(int id, int numClients, int numServers, int m) {
		this.id = id;
		this.numClients = numClients;
		this.numServers = numServers;
		this.slotCount = m;

		this.cipher = new SlotCipher(getSecrets());
		this.logger = Logger.getGlobal();
	}

	public void initializeConnections() throws IOException {
		// Start listening for incoming connections.
		ServerSocket serverSocket;
		ServerSocket clientSocket;
		try {
			serverSocket = new ServerSocket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(SERVER_PORT + id));

			clientSocket = new ServerSocket();
			clientSocket.setReuseAddress(true);
			clientSocket.bind(new InetSocketAddress(CLIENT_PORT + id));
		} catch (IOException e) {
			logger.severe("Exception creating listening sockets.");
			throw e;
		}

		// Initialize all server connections first; connect to servers
		// with a greater id and wait for connections from those with a
		// lesser id.
		serverSockets = new Socket[numServers - 1];
		for (int i = 0; i < id; i++) {
			try {
				Socket socket = new Socket("localhost", SERVER_PORT + i);
				serverSockets[i] = socket;
			} catch (UnknownHostException e) {
				logger.severe("Unknown server host.");
				throw e;
			}
		}
		for (int i = id + 1; i < numServers; i++) {
			try {
				Socket socket = serverSocket.accept();
				serverSockets[i - 1] = socket;
			} catch (IOException e) {
				logger.severe("Exception accepting server connection.");
				throw e;
			}
		}
		logger.info(String.format("%d/%d servers connected.",
					numServers - 1, numServers - 1));

		// Then wait for all expected clients to connect.
		clientSockets = new Socket[connectingClients()];
		for (int i = 0; i < connectingClients(); i++) {
			try {
				Socket socket = clientSocket.accept();
				clientSockets[i] = socket;
			} catch (IOException e) {
				logger.severe("Exception accepting client connection.");
				throw e;
			}
			if ((i+1) % 5 == 0 || (i+1) == connectingClients()) {
				logger.info(String.format("%d/%d clients connected.",
							(i+1), connectingClients()));
			}
		}
	}

	private void startProtocolRound() throws IOException {
		// Scratch space for slot data - re-used as needed.
		byte[] dataBuffer = new byte[Client.SLOT_LENGTH];

		// When the round started, used for periodic reporting.
		long first = System.currentTimeMillis();

		// Record all the transmitted slots for output later.
		byte[][] slotOutputs = new byte[slotCount][];

		// Simple statistics, to make sure it's working.
		int collisions = 0;
		int emptySlots = slotCount;

		for (int i = 0; i < slotCount; i++) {
			boolean slotEmpty = true;

			for (int j = 0; j < Client.ATTEMPTS; j++) {
				// Keeps the running total, initially XOR of secrets.
				byte[] slotBuffer = new byte[Client.SLOT_LENGTH];
				cipher.xorKeyStream(slotBuffer);

				// Get ciphertexts from all connected clients.
				for (Socket clientSocket : clientSockets) {
					InputStream is = clientSocket.getInputStream();
					DataInputStream dis = new DataInputStream(is);
					dis.readFully(dataBuffer);

					XORCipher.xorBytes(dataBuffer, slotBuffer);
				}
				
				// Send our aggregate ciphertext to the other servers.
				for (Socket serverSocket : serverSockets) {
					OutputStream os = serverSocket.getOutputStream();
					os.write(slotBuffer);
				}

				// Get the other servers' aggregate ciphertexts.
				for (Socket serverSocket : serverSockets) {
					InputStream is = serverSocket.getInputStream();
					DataInputStream dis = new DataInputStream(is);
					dis.readFully(dataBuffer);

					XORCipher.xorBytes(dataBuffer, slotBuffer);
				}

				// slotBuffer should now contain the plaintext. Do some
				// sanity checking on it, simplistically for now, and
				// then stash it away for writing out later.
				ByteBuffer wrapper = ByteBuffer.wrap(slotBuffer, 0, 12);
				final int length = wrapper.getInt();
				final long checksum = wrapper.getLong();

				if (length > 0) {
					CRC32 crc32 = new CRC32();
					crc32.update(slotBuffer, 12, slotBuffer.length - 12);
					if (crc32.getValue() != checksum) {
						logger.warning(String.format("Collision in slot %d.", i));
						collisions++;
					} else {
						slotOutputs[i] = slotBuffer;
						slotEmpty = false;
					}
				}
			}

			// Adjust the count of empty slots.
			emptySlots -= !slotEmpty ? 1 : 0;

			// Periodic debug/performance statistics.
			final int sampleInterval = 10;
			if ((i + 1) % sampleInterval == 0) {
				long now = System.currentTimeMillis();
				logger.log(Level.FINE, String.format("Slot #%d: %f slots/sec.", (i+1)/Client.ATTEMPTS,
							1000 * (((double) (i+1)) / (now - first)) / Client.ATTEMPTS));
			}
		}

		String outputFile = String.format("run/output/%d.csv", id);
		try (
			FileWriter fw = new FileWriter(outputFile);
			BufferedWriter bw = new BufferedWriter(fw);
		) {
			for (byte[] slot : slotOutputs) {
				if (slot != null) {
					ByteBuffer wrapper = ByteBuffer.wrap(slot, 0, 4);
					int length = wrapper.getInt();

					String elem = new String(slot, 12, length, "ISO-8859-1"); 
					bw.write(elem);
					bw.newLine();
				}
			}
		} catch (IOException e) {
			System.err.println("Error writing output to file.");
			throw e;
		}

		long now = System.currentTimeMillis();
		logger.info(String.format("%d bytes, %d ms, %d collisions, %d empty",
					Client.SLOT_LENGTH * slotCount, now - first, collisions, emptySlots));
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

		Logger.getGlobal().setLevel((id == 0) ? Level.FINE : Level.INFO);

		double[] params = BloomFilter.getParameterEstimate(Client.ELEMENTS, Client.FPR);
		int m = (int) params[0];

		Server server = new Server(id, clients, servers, m);
		try {
			server.initializeConnections();
			server.startProtocolRound();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
