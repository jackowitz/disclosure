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

import scheduler.SlotUtils;

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
				Socket socket = new Socket("localhost", SERVER_PORT + i);
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
		// Scratch space for slot data - re-used as needed.
		byte[] dataBuffer = new byte[Client.SLOT_LENGTH];

		// When the round started, used for periodic reporting.
		long first = System.currentTimeMillis();

		// Record all the transmitted slots for output later.
		byte[][] slotOutputs = new byte[slotCount][];

		// Simple statistics, to make sure it's working.
		int bytes = 0;
		int collisions = 0;
		int emptySlots = slotCount;

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
				SlotUtils.SlotMetadata meta = SlotUtils.decode(slotBuffer);
				if (meta.length > 0) {
					if (!meta.isValid) {
						logger.warning(String.format("Collision in slot %d.", i));
						collisions++;
					} else if (slotEmpty) {
						slotOutputs[i] = slotBuffer;
						slotEmpty = false;
					}
				}
				bytes += slotBuffer.length;

				// Send the plaintext back down to the clients, if needed.
				if (true) {
					for (Socket clientSocket : clientSockets) {
						OutputStream os = clientSocket.getOutputStream();
						os.write(slotBuffer);
					}
				}
			}

			// Adjust the count of empty slots.
			emptySlots -= !slotEmpty ? 1 : 0;
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
				System.err.println("Error writing output to file.");
				throw e;
			}
		}

		{ // Dump the final round statistics.
			long elapsed = System.currentTimeMillis() - first;
			String fmt = "slots=%d, bytes=%d, time=%d, collisions=%d, empty=%d";
			logger.info(String.format(fmt, slotCount, bytes, elapsed, collisions, emptySlots));
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
