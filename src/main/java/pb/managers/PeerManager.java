package pb.managers;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import pb.managers.endpoint.Endpoint;



/**
 * The Peer Manager manages both a number of ClientManagers and a ServerManager.
 * @author aaron
 *
 */
public class PeerManager extends Manager {
	private static Logger log = Logger.getLogger(PeerManager.class.getName());
	
	/**
	 * Events that this peer manager emits.
	 */
	
	/**
	 * Emitted when a session on the server manager is ready for use.
	 * <ul>
	 * <li>{@code args[0] instanceof Endpoint}</li>
	 * <li>{@code args[1] instanceof ServerManager}</li>
	 * </ul>
	 * Note that this event is also emitted on new client managers and
	 * in this case {@code args[1] instanceof ClientManager}
	 */
	public static final String peerStarted = "PEER_STARTED";
	
	/**
	 * Emitted when a session on the server manager
	 * has stopped and can longer be used.
	 * <ul>
	 * <li>{@code args[0] instanceof Endpoint}</li>
	 * <li>{@code args[1] instanceof ServerManager}</li>
	 * </ul>
	 * Note that this event is also emitted on new client managers and
	 * in this case {@code args[1] instanceof ClientManager}
	 */
	public static final String peerStopped = "PEER_STOPPED";
	
	/**
	 * Emitted when a session on a server manager has stopped
	 * in error and can no longer be used.
	 * <ul>
	 * <li>{@code args[1] instanceof ServerManager}</li>
	 * </ul>
	 * Note that this event is also emitted on new client managers and
	 * in this case {@code args[1] instanceof ClientManager}
	 */
	public static final String peerError = "PEER_ERROR";
	
	/**
	 * ServerManager has been initialized but not started.
	 * <ul>
	 * <li>{@code args[0] instanceof ServerManager}</li>
	 * </ul>
	 */
	public static final String peerServerManager = "PEER_SERVER_MANAGER";
	
	/**
	 * The client managers are for connecting to the server and other peers.
	 */
	private Set<ClientManager> clientManagers;
	
	/**
	 * The server manager is for accepting connections from other peers.
	 */
	private ServerManager serverManager;
	
	/**
	 * My server port
	 */
	private int myServerPort;
	
	/**
	 * Initialize with a port for the server manager for this peer
	 * to use.
	 * @param myServerPort
	 */
	public PeerManager(int myServerPort) {
		clientManagers = new HashSet<>();
		this.myServerPort=myServerPort;
	}
	
	/**
	 * 
	 * @return the server manager for this peer
	 */
	public ServerManager getServerManager() {
		return serverManager;
	}
	
	/**
	 * Connect to either a server or another peer. The client manager
	 * needs to be started after it is returned.
	 * @param serverPort the port of the server/peer to connect to
	 * @param host the hostname of the server/peer to connect to
	 * @throws InterruptedException 
	 * @throws UnknownHostException 
	 * @return the client manager for the new connection
	 */
	public ClientManager connect(int serverPort,String host) throws UnknownHostException, InterruptedException {
		ClientManager clientManager = new ClientManager(host,serverPort);
		clientManagers.add(clientManager);
		clientManager.on(ClientManager.sessionStarted, (args)->{
			Endpoint client = (Endpoint)args[0];
			clientManager.emit(peerStarted, client,clientManager);
		}).on(ClientManager.sessionStopped, (args)->{
			Endpoint client = (Endpoint)args[0];
			clientManager.emit(peerStopped, client,clientManager);
		}).on(ClientManager.sessionError, (args)->{
			Endpoint client = (Endpoint)args[0];
			clientManager.emit(peerError, client,clientManager);
		});
		return clientManager;
	}
	
	/**
	 * Close the server and all remaining connections.
	 * We will do a graceful shutdown here, to allow any other peers that
	 * are downloading files from this peer to complete first. User can ctrl+c
	 * if they want to quit immediately.
	 */
	@Override
	public void shutdown() {
		serverManager.shutdown();
		clientManagers.forEach((clientManager)->{
			clientManager.shutdown(); // client manager will send a session stop
		});
	}
	
	@Override
	public void run() {
		// initialize a server manager for other peers to connect to
		serverManager=new ServerManager(myServerPort);
		// setup the callbacks for when another peer connects to this peer
		serverManager.on(ServerManager.sessionStarted, (args)->{
			Endpoint client = (Endpoint)args[0];
			localEmit(peerStarted,client,serverManager);
		}).on(ServerManager.sessionStopped, (args)->{
			Endpoint client = (Endpoint)args[0];
			localEmit(peerStopped,client,serverManager);
		}).on(ServerManager.sessionError, (args)->{
			Endpoint client = (Endpoint)args[0];
			localEmit(peerError,client,serverManager);
		});
		localEmit(peerServerManager,serverManager);
		serverManager.start();
	}
	
	/**
	 * Join with any outstanding client managers, to ensure they have
	 * all completed. Only useful if the client managers are expected
	 * to terminate on their own, otherwise they should be explicitly
	 * shutdown using {@link #shutdown()} first.
	 */
	public void joinWithClientManagers() {
		clientManagers.forEach((clientManager)->{
			try {
				clientManager.join();
			} catch (InterruptedException e) {
				log.warning("could not join with client manager");
			}
		});
	}

}
