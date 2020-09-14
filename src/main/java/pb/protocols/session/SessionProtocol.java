package pb.protocols.session;

import java.util.logging.Logger;

import pb.managers.Manager;
import pb.managers.endpoint.Endpoint;
import pb.protocols.Message;
import pb.protocols.Protocol;
import pb.utils.Utils;
import pb.protocols.IRequestReplyProtocol;

/**
 * Allows the client to request the session to start and to request the session
 * to stop, which in turns allows the sockets to be properly closed at both
 * ends. Actually, either party can make such requests, but usually the client
 * would make the session start request as soon as it connects, and usually the
 * client would make the session stop request. The server may however send a
 * session stop request to the client if it wants (needs) to stop the session,
 * e.g. perhaps the server is becoming overloaded and needs to shed some
 * clients.
 * 
 * @see {@link pb.managers.Manager}
 * @see {@link pb.managers.endpoint.Endpoint}
 * @see {@link pb.Protocol}
 * @see {@link pb.protocols.IRequestReplyProtocol}
 * @see {@link pb.protocols.session.SessionStartRequest}
 * @see {@link pb.protocols.session.SessionStartReply}
 * @see {@link pb.protocols.session.SessionStopRequest}
 * @see {@link pb.protocols.session.SessionStopReply}
 * @author aaron
 *
 */
public class SessionProtocol extends Protocol implements IRequestReplyProtocol {
	private static Logger log = Logger.getLogger(SessionProtocol.class.getName());
	
	/**
	 * The unique name of the protocol.
	 */
	public static final String protocolName="SessionProtocol";
	
	/**
	 * Default request timeout
	 */
	private int sessionTimeout = 40000;
	
	// Use of volatile is in case the thread that calls stopProtocol is different
	// to the endpoint thread, although in this case it hardly needed.
	
	/**
	 * Whether the protocol has started, i.e. start request and reply have been sent,
	 * or not.
	 */
	private volatile boolean protocolRunning=false;
	
	/**
	 * Whether the protocol has been stopped.
	 */
	private volatile boolean stopped=false;
	
	/**
	 * Initialise the protocol with an endpoint and manager.
	 * @param endpoint
	 * @param manager
	 */
	public SessionProtocol(Endpoint endpoint, ISessionProtocolHandler manager) {
		super(endpoint,(Manager)manager);
	}
	
	/**
	 * @return the name of the protocol.
	 */
	@Override
	public String getProtocolName() {
		return protocolName;
	}

	/**
	 * If this protocol is stopped while it is still in the running
	 * state then this indicates something may be a problem.
	 */
	@Override
	public void stopProtocol() {
		if(protocolRunning) {
			log.severe("protocol stopped while it is still underway");
		}
		stopped=true;
	}
	
	/*
	 * Interface methods
	 */

	
	/**
	 * Called by the manager that is acting as a client. Timeout if
	 * a response is not seen.
	 */
	@Override
	public void startAsClient() {
		//  send the server a start session request
		sendRequest(new SessionStartRequest());
	}

	/**
	 * Called by the manager that is acting as a server.
	 */
	@Override
	public void startAsServer() {
		Utils.getInstance().setTimeout(()->{
			if(!stopped && !protocolRunning) {
				// we timed out
				manager.endpointTimedOut(endpoint, this);
			}
		}, sessionTimeout);
	}
	
	/**
	 * Generic stop session call, for either client or server.
	 */
	public void stopSession() {
		sendRequest(new SessionStopRequest());
	}
	
	/**
	 * Just send a request, nothing special.
	 * @param msg
	 */
	@Override
	public void sendRequest(Message msg) {
		endpoint.sendWithTimeout(msg,()->{
			// the message timed out
			if(!stopped)
			manager.endpointTimedOut(endpoint, this);
		},sessionTimeout);
	}

	/**
	 * If the reply is a session start reply then tell the manager that
	 * the session has started, otherwise if its a session stop reply then
	 * tell the manager that the session has stopped. If something weird 
	 * happens then tell the manager that something weird has happened.
	 * @param msg
	 */
	@Override
	public void receiveReply(Message msg) {
		if(msg instanceof SessionStartReply) {
			if(protocolRunning){
				// error, received a second reply?
				manager.protocolViolation(endpoint,this);
				return;
			}
			protocolRunning=true;
			((ISessionProtocolHandler)manager).sessionStarted(endpoint);
		} else if(msg instanceof SessionStopReply) {
			if(!protocolRunning) {
				// error, received a second reply?
				manager.protocolViolation(endpoint,this);
				return;
			}
			protocolRunning=false;
			((ISessionProtocolHandler)manager).sessionStopped(endpoint);
		}
	}

	/**
	 * If the received request is a session start request then reply and
	 * tell the manager that the session has started. If the received request
	 * is a session stop request then reply and tell the manager that
	 * the session has stopped. If something weird has happened then...
	 * @param msg
	 */
	@Override
	public void receiveRequest(Message msg) {
		if(msg instanceof SessionStartRequest) {
			if(protocolRunning) {
				// error, received a second request?
				manager.protocolViolation(endpoint,this);
				return;
			}
			protocolRunning=true;
			endpoint.sendAndCancelTimeout(new SessionStartReply(),msg);
			((ISessionProtocolHandler)manager).sessionStarted(endpoint);
		} else if(msg instanceof SessionStopRequest) {
			if(!protocolRunning) {
				// error, received a second request?
				manager.protocolViolation(endpoint,this);
				return;
			}
			protocolRunning=false;
			endpoint.sendAndCancelTimeout(new SessionStopReply(),msg);
			((ISessionProtocolHandler)manager).sessionStopped(endpoint);
		}
		
	}

	/**
	 * Just send a reply, nothing special to do.
	 * @param msg
	 */
	@Override
	public void sendReply(Message msg) {
		endpoint.send(msg);
	}

	

	
}
