package pb.managers;


import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import pb.managers.endpoint.Endpoint;
import pb.managers.endpoint.ProtocolAlreadyRunning;
import pb.protocols.IRequestReplyProtocol;
import pb.protocols.Protocol;
import pb.protocols.event.EventProtocol;
import pb.protocols.event.IEventProtocolHandler;
import pb.protocols.keepalive.IKeepAliveProtocolHandler;
import pb.protocols.keepalive.KeepAliveProtocol;
import pb.protocols.session.ISessionProtocolHandler;
import pb.protocols.session.SessionProtocol;

/**
 * Manages the connection to the server and the client's state.
 * 
 * @see {@link pb.managers.Manager}
 * @see {@link pb.managers.endpoint.Endpoint}
 * @see {@link pb.protocols.Protocol}
 * @see {@link pb.protocols.IRequestReplyProtocol}
 * @author aaron
 *
 */
public class ClientManager extends Manager implements ISessionProtocolHandler,
	IKeepAliveProtocolHandler, IEventProtocolHandler
{
	private static Logger log = Logger.getLogger(ClientManager.class.getName());
	
	/**
	 * Events emitted by the ClientManager
	 */
	
	/**
	 * Emitted when a session on an endpoint is ready for use.
	 * <ul>
	 * <li>{@code args[0] instanceof Endpoint}</li>
	 * </ul>
	 */
	public static final String sessionStarted="SESSION_STARTED";
	
	/**
	 * Emitted when a session has stopped and can longer be used.
	 * <ul>
	 * <li>{@code args[0] instanceof Endpoint}</li>
	 * </ul>
	 */
	public static final String sessionStopped="SESSION_STOPPED";
	
	/**
	 * Emitted when a session has stopped in error and can no longer
	 * be used.
	 * <ul>
	 * <li>{@code args[0] instanceof Endpoint}</li>
	 * </ul>
	 */
	public static final String sessionError="SESSION_ERROR";
	
	/**
	 * The session protocol for this client, so we can stop the
	 * session when we need to.
	 */
	private SessionProtocol sessionProtocol;
	
	/**
	 * The socket for this client.
	 */
	private Socket socket;
	
	/**
	 * The host to connect to.
	 */
	private String host;
	
	/**
	 * The host's port to connect to.
	 */
	private int port;
	
	/**
	 * When a connection fails, should we retry.
	 */
	private boolean shouldWeRetry=false;
	
	/**
	 * Initialise the client manage with a host and port to connect to.
	 * @param host
	 * @param port
	 * @throws UnknownHostException
	 * @throws InterruptedException
	 */
	public ClientManager(String host,int port) throws UnknownHostException, InterruptedException {
		this.host=host;
		this.port=port;
	}
	
	@Override
	public void shutdown() {
		sessionProtocol.stopSession();
	}
	
	@Override
	public void run() {
		int retries=10;
		while(retries-- > 0) {
			if(attemptToConnect(host,port)) {
				// the connection ended in error, so let's just
				// try to get it back up, transparently to the
				// higher layer
				try {
					Thread.sleep(5000); // short pause before retrying
				} catch (InterruptedException e) {
					continue;
				} 
			} else {
				// connection ended cleanly, so we can terminate this manager
				return;
			}
		}
		log.severe("no more retries, giving up");
		
	}
	/**
	 * Attempt to connect.
	 * @param host
	 * @param port
	 * @return true if we should retry to connect again or false otherwise
	 */
	private boolean attemptToConnect(final String host,final int port) {
		shouldWeRetry=false; // may be set to true by another thread
						     // if errors occur on the connection
		log.info("attempting to connect to "+host+":"+port);
		try {
			socket=new Socket(InetAddress.getByName(host),port);
			Endpoint endpoint = new Endpoint(socket,this);
			endpoint.start();

			try {
				// just wait for this thread to terminate
				endpoint.join();
			} catch (InterruptedException e) {
				// just make sure the endpoint has done everything it should
				endpoint.close();
			}
		} catch (UnknownHostException e) {
			return false; // we wont retry
		} catch (IOException e1) {
			shouldWeRetry=true;
		} finally {
			if(socket!=null)
				try {
					socket.close();
				} catch (IOException e) {
					//ignore
				}
		}
		return shouldWeRetry;
	}
	
	/**
	 * The endpoint is ready to use.
	 * @param endpoint
	 */
	@Override
	public void endpointReady(Endpoint endpoint) {
		log.info("connection with server established");
		sessionProtocol = new SessionProtocol(endpoint,this);
		try {
			// we need to add it to the endpoint before starting it
			endpoint.handleProtocol(sessionProtocol);
			sessionProtocol.startAsClient();
		} catch (ProtocolAlreadyRunning e) {
			// hmmm, so the server is requesting a session start?
			log.warning("server initiated the session protocol... weird");
		}
		KeepAliveProtocol keepAliveProtocol = new KeepAliveProtocol(endpoint,this);
		try {
			// we need to add it to the endpoint before starting it
			endpoint.handleProtocol(keepAliveProtocol);
			keepAliveProtocol.startAsClient();
		} catch (ProtocolAlreadyRunning e) {
			// hmmm, so the server is requesting a session start?
			log.warning("server initiated the session protocol... weird");
		}
	}
	
	/**
	 * The endpoint close() method has been called and completed.
	 * @param endpoint
	 */
	public void endpointClosed(Endpoint endpoint) {
		log.info("connection with server terminated");
	}
	
	/**
	 * The endpoint has abruptly disconnected. It can no longer
	 * send or receive data.
	 * @param endpoint
	 */
	@Override
	public void endpointDisconnectedAbruptly(Endpoint endpoint) {
		log.severe("connection with server terminated abruptly");
		localEmit(sessionError,endpoint);
		endpoint.close();
		shouldWeRetry=true;
	}

	/**
	 * An invalid message was received over the endpoint.
	 * @param endpoint
	 */
	@Override
	public void endpointSentInvalidMessage(Endpoint endpoint) {
		log.severe("server sent an invalid message");
		localEmit(sessionError,endpoint);
		endpoint.close();
	}
	

	/**
	 * The protocol on the endpoint is not responding.
	 * @param endpoint
	 */
	@Override
	public void endpointTimedOut(Endpoint endpoint,Protocol protocol) {
		log.severe("server has timed out");
		localEmit(sessionError,endpoint);
		endpoint.close();
		shouldWeRetry=true;
	}

	/**
	 * The protocol on the endpoint has been violated.
	 * @param endpoint
	 */
	@Override
	public void protocolViolation(Endpoint endpoint,Protocol protocol) {
		log.severe("protocol with server has been violated: "+protocol.getProtocolName());
		localEmit(sessionError,endpoint);
		endpoint.close();
	}

	/**
	 * The session protocol is indicating that a session has started.
	 * @param endpoint
	 */
	@Override
	public void sessionStarted(Endpoint endpoint) {
		log.info("session has started with server");
		
		EventProtocol eventProtocol = new EventProtocol(endpoint,this);
		try {
			endpoint.handleProtocol(eventProtocol);
			eventProtocol.startAsServer();
		} catch (ProtocolAlreadyRunning e) {
			// hmmm... already requested by the client
		}
		
		localEmit(sessionStarted,endpoint);
	}

	/**
	 * The session protocol is indicating that the session has stopped. 
	 * @param endpoint
	 */
	@Override
	public void sessionStopped(Endpoint endpoint) {
		log.info("session has stopped with server");
		localEmit(sessionStopped,endpoint);
		endpoint.close(); // this will stop all the protocols as well
	}
	

	/**
	 * The endpoint has requested a protocol to start. If the protocol
	 * is allowed then the manager should tell the endpoint to handle it
	 * using {@link pb.managers.endpoint.Endpoint#handleProtocol(Protocol)}
	 * before returning true.
	 * @param protocol
	 * @return true if the protocol was started, false if not (not allowed to run)
	 */
	@Override
	public boolean protocolRequested(Endpoint endpoint, Protocol protocol) {
		// the only protocols in this system are this kind...
		try {
			((IRequestReplyProtocol)protocol).startAsClient();
			endpoint.handleProtocol(protocol);
			return true;
		} catch (ProtocolAlreadyRunning e) {
			// even more weird... should log this too
			return false;
		}
	}
}
