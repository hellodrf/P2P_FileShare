package pb.managers;

import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
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
 * Manages all of the clients for the server and the server's state.
 * 
 * @see {@link pb.managers.Manager}
 * @see {@link pb.managers.IOThread}
 * @see {@link pb.managers.endpoint.Endpoint}
 * @see {@link pb.protocols.Protocol}
 * @see {@link pb.protocols.IRequestReplyProtocol}
 * @author aaron
 *
 */
public class ServerManager extends Manager implements ISessionProtocolHandler,
	IKeepAliveProtocolHandler, IEventProtocolHandler
{
	private static Logger log = Logger.getLogger(ServerManager.class.getName());
	
	/**
	 * Events emitted by the ServerManager
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
	 * TODO for project 2A. Make the server listen to these events
	 * on all client connections and call the appropriate shutdown method
	 * if the received password matches the password for this server.
	 * Events the server will listen for.
	 */
	
	/**
	 * Emitted to cause the server to shutdown. Single argument
	 * is the password to check.
	 * <ul>
	 * <li>{@code args[0] instanceof String}</li>
	 * </ul>
	 */
	public static final String shutdownServer="SHUTDOWN_SERVER";
	
	/**
	 * Emitted to cause the server to force shutdown. Single argument
	 * is the password to check.
	 * <ul>
	 * <li>{@code args[0] instanceof String}</li>
	 * </ul>
	 */
	public static final String forceShutdownServer="FORCE_SHUTDOWN_SERVER";
	
	/**
	 * Emitted to cause the server to vader shutdown. Single argument
	 * is the password to check.
	 * <ul>
	 * <li>{@code args[0] instanceof String}</li>
	 * </ul>
	 */
	public static final String vaderShutdownServer="VADER_SHUTDOWN_SERVER";
	
	/**
	 * The io thread accepts connections and informs the server manager
	 * of the connection's socket.
	 */
	private IOThread ioThread;
	
	/**
	 * Keep a track of endpoints that
	 * have not yet terminated, so that we can wait/ask/force for them to finish
	 * before completely terminating. This object can be called by multiple
	 * endpoint threads and this server manager thread; so synchronized is needed.
	 */
	private final Set<Endpoint> liveEndpoints;
	
	/**
	 * The port for this server.
	 */
	private final int port;
	
	/**
	 * Should we force shutdown, i.e force endpoints to close.
	 */
	private volatile boolean forceShutdown=false;
	
	/**
	 * Should we force shutdown and not even wait for endpoints to close.
	 */
	private volatile boolean vaderShutdown=false;
	
	/**
	 * Initialise the ServerManager with a port number for the io thread to listen on.
	 * @param port to use when creating the io thread
	 */
	public ServerManager(int port) {
		this.port=port;
		liveEndpoints=new HashSet<>();
		setName("ServerManager"); // name the thread, urgh simple log can't print it :-(
	}
	
	/**
	 * TODO: for Project 2B. Create an initializer that does as above but also takes
	 * a password as an argument.
	 */
	
	/**
	 * TODO: for Project 2B. Use one of these methods appropriately for the event
	 * emitted, when your server receives a correct password. Usually a single
	 * shutdown method would suffice, but for servers it is convenient to have
	 * different methods, depending on how the administrator wants to shut the
	 * server down, like if they are in a rush, or can wait for existing clients to
	 * finish up gracefully, or if they can't wait at all, etc.
	 */
	
	public void shutdown() {
		log.info("server shutdown called");
		// this will not force existing clients to finish their sessions
		ioThread.shutDown();
	}
	
	public void forceShutdown() { // Skywalker style :-)
		log.warning("server force shutdown called");
		forceShutdown=true; // this will send session stops to all the clients
		ioThread.shutDown();
	}
	
	public void vaderShutdown() { // Darkside style :-]
		log.warning("server vader shutdown called");
		vaderShutdown=true; // this will just close all of the endpoints abruptly
		ioThread.shutDown();
	}
	
	/**
	 * Convenience wrapper
	 * @return the number of live endpoints
	 */
	public int numLiveEndpoints() {
		synchronized(liveEndpoints) {
			return liveEndpoints.size();
		}
	}
	
	@Override
	public void run() {
		log.info("started");
		// when the IO thread terminates, and all endpoints have terminated,
		// then the server will terminate
		try {
			ioThread = new IOThread(port,this);
		} catch (IOException e1) {
			log.severe("could not start the io thread");
			return;
		}
		
		try {
			// just wait for this thread to terminate
			ioThread.join();
		} catch (InterruptedException e) {
			// just make sure the ioThread is going to terminate
			ioThread.shutDown();
		}
		
		log.info("io thread has joined");
		
		// At this point, there still may be some endpoints that have not
		// terminated, and so the JVM will remain running until they do.
		// However no new endpoints can be created.
		
		// if we want to tell clients to end session
		// it is indeed possible that both may be set true
		if(forceShutdown && !vaderShutdown) {
			// let's send a stop session to existing clients
			synchronized(liveEndpoints) {
				liveEndpoints.forEach((endpoint)->{
					SessionProtocol sessionProtocol=(SessionProtocol) endpoint.getProtocol("SessionProtocol");
					if(sessionProtocol!=null)
						sessionProtocol.stopSession();
				});
			}
		}
		
		// in this case we just close the endpoints, which will likely cause
		// abrupt disconnection
		if(vaderShutdown) {
			// let's just close everything
			synchronized(liveEndpoints) {
				liveEndpoints.forEach((endpoint)->{
					endpoint.close();
				});
			}
		}
		
		// let's wait for the remaining clients if we can
		while(numLiveEndpoints()>0 && !vaderShutdown) {
			log.warning("still waiting for "+numLiveEndpoints()+" to finish");
			try {
				Thread.sleep(1000); // just wait a little longer
			} catch (InterruptedException e) {
				if(numLiveEndpoints()>0) {
					log.severe("terminating server with "+numLiveEndpoints()+
							" still unfinished");
				}
				break;
			}
			if(vaderShutdown) {
				// maybe we missed some earlier
				synchronized(liveEndpoints) {
					liveEndpoints.forEach((endpoint)->{
						endpoint.close();
					});
				}
			}
		}
		log.info("terminated");
	}
	
	/**
	 * A new client has connected to the server. We need to keep
	 * a set of all clients that have connected, so that we can
	 * do global operations, like broadcast data to all clients.
	 * @param clientSocket the socket connection for the client.
	 */
	public void acceptClient(Socket clientSocket) {
		Endpoint endpoint = new Endpoint(clientSocket,this);
		endpoint.start();
	}
	
	/**
	 * Called by a client endpoint to signal that it is now ready for
	 * use, the server can send data and it may start receiving messages
	 * from the client, etc. The server will now start the KeepAlive protocol
	 * so as to detect clients that are dead. The server will wait for the
	 * client to start the session protocol, or else terminate the connection
	 * if it does not stay alive.
	 * @param endpoint
	 */
	@Override
	public void endpointReady(Endpoint endpoint) {
		if(vaderShutdown) {
			endpoint.close(); // we'll kill it here
			return;
		}
		synchronized(liveEndpoints) {
			liveEndpoints.add(endpoint);
		}
		
		/*
		 * TODO: For Project2B Insert code here to ensure that the server listens on the
		 * endpoint for shutdown events and calls the appropriate method. Use simple
		 * authentication, where the string data for the event contains a secret that
		 * that is provided by the client on its command line, and provided on the
		 * command line when the server is running. If the secrets match then the
		 * shutdown is issued, otherwise it is ignored.
		 */
		
		KeepAliveProtocol keepAliveProtocol = new KeepAliveProtocol(endpoint,this);
		try {
			// we need to add it to the endpoint before starting it
			endpoint.handleProtocol(keepAliveProtocol);
			keepAliveProtocol.startAsServer();
		} catch (ProtocolAlreadyRunning e) {
			// hmmm... already requested by the client
		}
		SessionProtocol sessionProtocol = new SessionProtocol(endpoint,this);
		try {
			endpoint.handleProtocol(sessionProtocol);
			sessionProtocol.startAsServer();
		} catch (ProtocolAlreadyRunning e) {
			// hmmm... already started by the client
		}
		
		
	}
	
	/**
	 * The endpoint close() method has been called and completed.
	 * @param endpoint
	 */
	@Override
	public void endpointClosed(Endpoint endpoint) {
		synchronized(liveEndpoints) {
			liveEndpoints.remove(endpoint);
		}
	}

	/**
	 * The session has started for this client endpoint. Other protocols
	 * may now be started, etc. We will start the event protocol now.
	 * @param endpoint
	 */
	@Override
	public void sessionStarted(Endpoint endpoint) {
		log.info("session has started with client: "+endpoint.getOtherEndpointId());
		
		if(forceShutdown) {
			// ask the client to stop now
			SessionProtocol sessionProtocol=(SessionProtocol) endpoint.getProtocol("SessionProtocol");
			if(sessionProtocol!=null)
				sessionProtocol.stopSession();
		}
		
		// now start the event protocol
		EventProtocol eventProtocol = new EventProtocol(endpoint,this);
		try {
			endpoint.handleProtocol(eventProtocol);
			eventProtocol.startAsServer();
		} catch (ProtocolAlreadyRunning e) {
			// hmmm... already requested by the client
		}
		
		// the event protocol has started but still no events
		// could have been received at this point
		localEmit(sessionStarted,endpoint);
		
	}

	/**
	 * The session has been stopped (usually by the client). The session should
	 * be last protocol to stop, other than the KeepAlive protocol. Server should now
	 * clean up any data relating to the client and any remaining protocols
	 * should be stopped, e.g. KeepAlive.
	 * @param endpoint
	 */
	@Override
	public void sessionStopped(Endpoint endpoint) {
		log.info("session has stopped with client: "+endpoint.getOtherEndpointId());
		
		localEmit(sessionStopped,endpoint);
		
		// we can now signal the client endpoint to close and forget this client
		endpoint.close(); // will stop all remaining protocols
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
			((IRequestReplyProtocol)protocol).startAsServer();
			endpoint.handleProtocol(protocol);
			return true;
		} catch (ProtocolAlreadyRunning e) {
			// even more weird...
			return true;
		}
		
	}

	
	/*
	 * Everything below here is handling error conditions that could
	 * arise with the client connection. Typically on error we terminate
	 * connection with the client, and we may need to clean up other stuff
	 * as well.
	 */

	/**
	 * The client has violated one of the protocols. Usual practice is to
	 * terminate the client connection.
	 * @param endpoint
	 * @param protocol
	 */
	@Override
	public void protocolViolation(Endpoint endpoint, Protocol protocol) {
		log.severe("client "+endpoint.getOtherEndpointId()+" violated the protocol "+protocol.getProtocolName());
		localEmit(sessionError,endpoint);
		endpoint.close();
	}
	
	/**
	 * The client connection died without warning. 
	 * Server needs to clean up client data and possibly recover
	 * from any faults that may occur due to this.
	 * @param endpoint
	 */
	@Override
	public void endpointDisconnectedAbruptly(Endpoint endpoint) {
		log.severe("client disconnected abruptly "+endpoint.getOtherEndpointId());
		localEmit(sessionError,endpoint);
		endpoint.close();
	}
	
	/**
	 * The client sent a message that is invalid. Usual practice is to 
	 * terminate the client connection.
	 * @param endpoint
	 */
	@Override
	public void endpointSentInvalidMessage(Endpoint endpoint) {
		log.severe("client sent an invalid message "+endpoint.getOtherEndpointId());
		localEmit(sessionError,endpoint);
		endpoint.close();
	}

	/**
	 * The client has timed out.
	 * Usual practice is to terminate the client connection.
	 * @param endpoint
	 * @param protocol
	 */
	@Override
	public void endpointTimedOut(Endpoint endpoint, Protocol protocol) {
		log.severe("client "+endpoint.getOtherEndpointId()+" has timed out on protocol "+protocol.getProtocolName());
		localEmit(sessionError,endpoint);
		endpoint.close();
	}

	

	
}
