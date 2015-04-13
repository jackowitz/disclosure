package dcnet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import dcnet.XORCipher;

public class SocketUtils {
	public static void read(byte[] buffer, byte[] scratch, Socket... sockets) throws IOException {
		for (Socket socket : sockets) {
			InputStream is = socket.getInputStream();
			DataInputStream dis = new DataInputStream(is);
			dis.readFully(scratch);

			if (buffer != null) // XXX hacky workaround
				XORCipher.xorBytes(scratch, buffer);
		}
	}

	public static void write(byte[] buffer, Socket... sockets) throws IOException {
		for (Socket socket : sockets) {
			OutputStream os = socket.getOutputStream();
			os.write(buffer);
		}
	}
}
