package pb.protocols.keepalive;

import java.time.Instant;
import java.util.logging.Logger;

import pb.managers.Manager;
import pb.managers.endpoint.Endpoint;
import pb.protocols.Message;
import pb.protocols.Protocol;
import pb.utils.Utils;
import pb.protocols.IRequestReplyProtocol;

/**
 * Provides all of the protocol logic for both client and server to undertake
 * the KeepAlive protocol. In the KeepAlive protocol, the client sends a
 * KeepAlive request to the server every {@link #keepAliveInterval} seconds using
 * {@link pb.utils.Utils#setTimeout(pb.protocols.ICallback, long)}. The server must
 * send a KeepAlive response to the client upon receiving the request. If the
 * client does not receive the response within {@link #keepAliveTimeout} seconds
 * it will assume the server is dead
 * and signal its manager using
 * {@link pb.managers.Manager#endpointTimedOut(Endpoint,Protocol)}. If the server does
 * not receive a KeepAlive request at least every {@link #keepAliveTimeout} seconds (again using
 * {@link pb.utils.Utils#setTimeout(pb.protocols.ICallback, long)}), it will assume
 * the client is dead and signal its manager. Upon initialisation, the client
 * should send the KeepAlive request immediately, whereas the server will wait
 * up to {@link #keepAliveTimeout} seconds before it assumes the client is dead. The protocol stops
 * when a timeout occurs.
 * 
 * @see {@link pb.managers.Manager}
 * @see {@link pb.managers.endpoint.Endpoint}
 * @see {@link pb.protocols.Message}
 * @see {@link pb.protocols.keepalive.KeepAliveRequest}
 * @see {@link pb.protocols.keepalive.KeepaliveRespopnse}
 * @see {@link pb.protocols.Protocol}
 * @see {@link pb.protocols.IRequestReqplyProtocol}
 * @author aaron
 *
 */
public class KeepAliveProtocol extends Protocol implements IRequestReplyProtocol {
	@SuppressWarnings("unused")
	private static Logger log = Logger.getLogger(KeepAliveProtocol.class.getName());

	/**
	 * Name of this protocol. 
	 */
	public static final String protocolName="KeepAliveProtocol";
	
	/**
	 * Default keep alive request interval
	 */
	private int keepAliveRequestInterval = 20000;
	
	/**
	 * Default keep alive timeout
	 */
	private int keepAliveTimeout = 40000;
	
	// Use of volatile is because the timer thread is different to the endpoint thread
	// and they make use of the same flags/variables.
	
	/**
	 * Time that a request was last sent.
	 */
	private volatile long timeReplySeen;
	
	/**
	 * Time that a request was last seen.
	 */
	private volatile long timeRequestSeen;
	
	
	/**
	 * Set to true to avoid any further timeouts. 
	 */
	private volatile boolean stopped=false;
	
	/**
	 * Whether we should timeout or not.
	 */
	private volatile boolean timeout=false; 
	
	/**
	 * Initialise the protocol with an endopint and a manager.
	 * @param endpoint
	 * @param manager
	 */
	public KeepAliveProtocol(Endpoint endpoint, IKeepAliveProtocolHandler manager) {
		super(endpoint,(Manager)manager);
	}
	
	/**
	 * @return the name of the protocol
	 */
	@Override
	public String getProtocolName() {
		return protocolName;
	}

	/**
	 * Just set a flag to avoid any further timeout callbacks.
	 */
	@Override
	public void stopProtocol() {
		stopped=true;
	}
	
	/*
	 * Interface methods
	 */
	
	/**
	 * Called by the manager that is acting as the server. Basically
	 * just wait for {@link #keepAliveTimeout} seconds and if no (new) request has been seen
	 * then timeout. Keep doing this until cancelled.
	 */
	public void startAsServer() {
		timeRequestSeen = Instant.now().toEpochMilli();
		// set a timeout callback
		Utils.getInstance().setTimeout(()->{
			checkClientTimeout();
		}, keepAliveTimeout);
	}
	
	/**
	 * callback to check for client timeout
	 */
	public void checkClientTimeout() {
		if(stopped)return;
		long now = Instant.now().toEpochMilli();
		if(now-timeRequestSeen > keepAliveTimeout) {
			// timeout :-(
			manager.endpointTimedOut(endpoint,this);
			stopProtocol();
		} else {
			// set a timeout callback
			Utils.getInstance().setTimeout(()->{
				checkClientTimeout();
			}, keepAliveTimeout);
		}
	}
	
	/**
	 * Called by the manager that is acting as the client. Basically
	 * send a keep alive immediately and timeout if no response within
	 * {@link #keepAliveTimeout} seconds.
	 * Keep doing this every {@link #keepAliveRequestInterval} seconds until cancelled.
	 */
	public void startAsClient() {
		// assume we saw a reply already
		timeReplySeen = Instant.now().toEpochMilli();
		// send a request straight away
		sendAnotherRequest();	
	}
	
	/**
	 * callback to send new request
	 */
	public void sendAnotherRequest() {
		if(stopped)return;
		sendRequest(new KeepAliveRequest());
		final long timeSent = Instant.now().toEpochMilli();
		Utils.getInstance().setTimeout(()->{
			sendAnotherRequest();
		}, keepAliveRequestInterval);
		Utils.getInstance().setTimeout(()->{
			checkServerTimeout(timeSent);
		}, keepAliveTimeout);
	}
	
	/**
	 * callback to check for server timeout
	 */
	public void checkServerTimeout(long timeSent) {
		if(stopped)return;
		if(timeout) {
			manager.endpointTimedOut(endpoint,this);
			stopProtocol();
		} else {
			if(timeReplySeen-timeSent > keepAliveTimeout) {
				//we timed out :-(
				timeout=true;
				manager.endpointDisconnectedAbruptly(endpoint);				
			} 
		}
	}

	/**
	 * Send a keep alive request.
	 * @param msg
	 */
	@Override
	public void sendRequest(Message msg) {
		KeepAliveRequest keepAliveRequest = (KeepAliveRequest) msg;
		endpoint.send(keepAliveRequest);
	}

	/**
	 * If we receive a keep alive reply, make a note of the time.
	 * @param msg
	 */
	@Override
	public void receiveReply(Message msg) {
		@SuppressWarnings("unused")
		KeepAliveReply keepAliveResponse = (KeepAliveReply) msg;
		timeReplySeen = Instant.now().toEpochMilli();
	}

	/**
	 * Received a keep alive request so make a note of when that was.
	 * @param msg
	 */
	@Override
	public void receiveRequest(Message msg) {
		@SuppressWarnings("unused")
		KeepAliveRequest keepAliveRequest = (KeepAliveRequest) msg;
		timeRequestSeen = Instant.now().toEpochMilli();
		sendReply(new KeepAliveReply());
	}

	/**
	 * Simply send a reply to a keep alive request.
	 * @param msg
	 */
	@Override
	public void sendReply(Message msg) {
		KeepAliveReply keepAliveResponse = (KeepAliveReply) msg;
		endpoint.send(keepAliveResponse);
	}
	
	
}
