package pb.managers;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Logger;

/**
 * Listen for connections on a given port number and pass them to the
 * {@link pb.managers.ServerManager} using
 * {@link pb.managers.ServerManager#acceptClient(Socket)}. Note that the
 * {@link pb.managers.ServerManager} is responsible for creating a thread for this
 * connection, else the IOThread will not accept any more connections until this
 * connection is finished.
 * 
 * @see {@link pb.managers.ServerManager}
 * @author aaron
 *
 */
public class IOThread extends Thread {
	private static Logger log = Logger.getLogger(IOThread.class.getName());
	private ServerSocket serverSocket=null;
	private int port;
	private ServerManager serverManager;
	
	/**
	 * Emitted when the io thread has started. The argument
	 * provides the io thread's Internet address in the 
	 * form "host:port"
	 * <ol>
	 * <li>{@code args[0] instanceof String}</li>
	 * </ol>
	 */
	public static final String ioThread = "IO_THREAD";
	
	/**
	 * Initialise the IOThread with a port number to listen on and reference
	 * to the {@link pb.managers.ServerManager}.
	 * @param port to listen on
	 * @param serverManager to send connections to
	 * @throws IOException whenever the server socket can't be created
	 */
	public IOThread(int port, ServerManager serverManager) throws IOException{
		serverSocket = new ServerSocket(port); // let's throw this since its potentially unrecoverable
		this.port=port;
		this.serverManager=serverManager;
		setName("IOThread");
		start();
	}
	
	/**
	 * Close the server socket and make sure the thread terminates.
	 */
	public void shutDown() {
		if(serverSocket!=null)
			try {
				serverSocket.close();
			} catch (IOException e) {
				log.warning("exception closing server socket: "+e.getMessage());
			}
		interrupt();
	}
	
	/**
	 * Listen for connections and pass them to the ServerManager.
	 */
	@Override
	public void run() {
		log.info("listening for connections on port "+port);
		try {
			serverManager.emit(ioThread,InetAddress.getLocalHost().getHostAddress()+":"+port);
		} catch (UnknownHostException e1) {
			log.severe("Could not get address of local host, continuing anyway, assuming 127.0.0.1");
			serverManager.emit(ioThread,"127.0.0.1:"+port);
		}
		while(!isInterrupted() && !serverSocket.isClosed()){
			Socket clientSocket;
			try {
				clientSocket = serverSocket.accept();
				log.info("Received connection from "+clientSocket.getInetAddress());
				serverManager.acceptClient(clientSocket);
			} catch (IOException e) {
				log.warning("exception accepting connection: "+e.getMessage());
			} 
		}
		log.info("IOThread terminating");
		try {
			serverSocket.close();
		} catch (IOException e) {
			log.warning("exception closing server socket: "+e.getMessage());
		}
	}
}
