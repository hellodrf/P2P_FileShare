package pb.managers.endpoint;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import pb.utils.Eventable;
import pb.utils.Utils;
import pb.protocols.InvalidMessage;
import pb.protocols.Message;
import pb.protocols.Protocol;
import pb.protocols.event.EventProtocol;
import pb.protocols.event.IEventProtocolHandler;
import pb.protocols.ICallback;
import pb.protocols.IRequestReplyProtocol;
import pb.protocols.keepalive.IKeepAliveProtocolHandler;
import pb.protocols.keepalive.KeepAliveProtocol;
import pb.protocols.session.ISessionProtocolHandler;
import pb.protocols.session.SessionProtocol;

/**
 * The endpoint is a thread that blocking reads incoming messages (on a socket)
 * and sends them to the appropriate protocol for processing; thus a
 * thread-per-connection model is being used. It also provides a synchronized
 * method to send data to the socket which will be sent to the other endpoint.
 * Any number of protocols can be handled by the endpoint, but there can be only
 * one instance of each protocol running at a time.
 * 
 * @see {@link pb.managers.Manager}
 * @see {@link pb.protocols.session.SessionProtocol}
 * @see {@link pb.protocols.keepalive.KeepAliveProtocol}
 * @author aaron
 *
 */
public class Endpoint extends Eventable {
	private static Logger log = Logger.getLogger(Endpoint.class.getName());
	
	/**
	 * The socket this endpoint is wrapped around.
	 */
	private Socket socket;
	
	/**
	 * The manager to report to when things happen.
	 */
	private IEndpointHandler manager;
	
	/**
	 * The input data stream on the socket.
	 */
	private DataInputStream in=null;
	
	/**
	 * The output data stream on the socket.
	 */
	private DataOutputStream out=null;
	
	/**
	 * A protocol name to protocol map, of protocols in use.
	 */
	private Map<String,Protocol> protocols;
	
	/**
	 * Timeout id to use.
	 */
	private long timeoutId=1;
	
	/**
	 * Oustanding ids
	 */
	private Set<Long> outstandingIds;
	
	/**
	 * stopped flag
	 */
	private volatile boolean stopped=true; // the use of send will return false always
	
	/**
	 * Initialise the endpoint with a socket and a manager.
	 * @param socket
	 * @param manager
	 */
	public Endpoint(Socket socket, IEndpointHandler manager) {
		this.socket = socket;
		this.manager = manager;
		protocols = new HashMap<>();
		outstandingIds = new HashSet<>();
		setName("Endpoint"); // name the thread
	}
	
	/**
	 * Send a Message on the socket for this endpoint. This is synchronized
	 * to avoid multiple concurrent messages overwriting each other on the socket.
	 * @param msg
	 * @return true if the message was sent, false otherwise
	 */
	public synchronized boolean send(Message msg) {
		if(stopped) return false;
		try {
			log.info("sending "+msg.getName()+" for protocol "+msg.getProtocolName()+" to "+getOtherEndpointId());
			out.writeUTF(msg.toJsonString());
			out.flush();
		} catch (IOException e) {
			manager.endpointDisconnectedAbruptly(this);
			return false;
		}
		return true;
	}
	
	/**
	 * Send a message and attach a timeout identifier to it. The callback
	 * is triggered if no reply to the message was seen within the given
	 * time interval.
	 * @param msg
	 * @param timeoutCallback
	 * @param timeInterval
	 * @return true if the message was sent and false otherwise
	 */
	public synchronized boolean sendWithTimeout(Message msg,
			ICallback timeoutCallback,int timeInterval) {
		long nextId = timeoutId++;
		synchronized(outstandingIds) {
			outstandingIds.add(nextId);
		}
		msg.setTimeoutId(nextId);
		boolean sent=send(msg);
		if(!sent) return false;
		Utils.getInstance().setTimeout(()->{
			boolean timedout;
			synchronized(outstandingIds) {
				timedout=outstandingIds.contains(nextId);
			}
			if(timedout) timeoutCallback.callback();
		}, timeInterval);
		return sent;
	}
	
	/**
	 * Send a message in reply to a message that has a timeout id associated
	 * with it. If it is received in time then it will ensure that a timeout
	 * does not occur.
	 * @param msg
	 * @param replyingTo
	 * @return true if the message was sent and false otherwise
	 */
	public synchronized boolean sendAndCancelTimeout(Message msg,
			Message replyingTo) {
		msg.setTimeoutId(replyingTo.getTimeoutId());
		return(send(msg));
	}
	
	/**
	 * Closes the endpoint, which closes the socket. Both the endpoint thread
	 * and the timer thread may end up attempting to do this in the event that
	 * they detect problems.
	 */
	public synchronized void close() {
		// we are stopping this endpoint, the send method will return false always now.
		stopped=true;
		/* 
	    * Tell all of the protocols to stop - they may not be able to correctly complete
		* their intended function however - and this should be flagged as an error
		* if it is the case.
		*/
		Set<String> protocolNames;
		synchronized(protocols) {
			protocolNames = new HashSet<String>(protocols.keySet());
		}
		if(protocolNames!=null)
			protocolNames.forEach((protocolName)->{stopProtocol(protocolName);});
		
		/*
		 *  The endpoint thread itself will not process any more messages if we
		 *  interrupt it.
		 *  Note that it currently may be processing a message, indeed it may
		 *  be this thread and interrupting itself.
		 */
		interrupt();
		
		/**
		 * At this point there may be exactly one _currently executing_ timer
		 * thread callback (which is a pain, but it can't be inside the
		 * send methods because these methods are synchronized), plus there may
		 * be pending timer thread callbacks that will want to use this endpoint
		 * (which wont run since protocol stopped has been set in the protocols).
		 * The endpoint is at this point just "closing", not closed.
		 */
		
		try {
			if(out!=null) out.close();
			out=null;
		} catch (IOException e) {
			log.warning("connection did not close properly: "+e.getMessage());
		}
		try {
			socket.close();
		} catch (IOException e) {
			log.warning("socket did not close properly: "+e.getMessage());
		}
		manager.endpointClosed(this);
	}
	
	/**
	 * Continue to read messages from the socket until interrupted.
	 */
	@Override
	public void run() {
		try {
			in = new DataInputStream(socket.getInputStream());
			out = new DataOutputStream(socket.getOutputStream());
		} catch (IOException e){
			manager.endpointDisconnectedAbruptly(this);
			return;
		}
		stopped=false; // allow use of the out stream
		manager.endpointReady(this);
		log.info("endpoint has started to: "+getOtherEndpointId());
		while(!isInterrupted()) {
			try {
				String line=in.readUTF();
				Message msg = Message.toMessage(line);
				// cancel any related time out
				if(msg.getType()==Message.Type.Reply) {
					synchronized(outstandingIds) {
						outstandingIds.remove(msg.getTimeoutId());
					}
				}
				// find the protocol
				Protocol protocol=null;
				synchronized(protocols) {
					protocol=protocols.get(msg.getProtocolName());
				}
				if(protocol==null) {
					switch(msg.getProtocolName()) {
					case SessionProtocol.protocolName:
						protocol=new SessionProtocol(this,(ISessionProtocolHandler)manager);
						break;
					case KeepAliveProtocol.protocolName:
						protocol=new KeepAliveProtocol(this,(IKeepAliveProtocolHandler)manager);
						break;
					case EventProtocol.protocolName:
						protocol=new EventProtocol(this,(IEventProtocolHandler)manager);
					}
					if(!manager.protocolRequested(this,protocol)) {
						log.info("message dropped due to no protocol available: "+line);
						continue;
					}
				}
				log.info("received "+msg.getName()+" for protocol "+msg.getProtocolName()+" from "+getOtherEndpointId());
				switch(msg.getType()) {
				case Request:
					((IRequestReplyProtocol)protocol).receiveRequest(msg);
					break;
				case Reply:
					((IRequestReplyProtocol)protocol).receiveReply(msg);
					break;
				}
			} catch (IOException e) {
				manager.endpointDisconnectedAbruptly(this);
				// we can't continue here
				break;
			} catch (InvalidMessage e) {
				manager.endpointSentInvalidMessage(this);
				// up to the client what to do
			}
		}
		try {
			in.close();
		} catch (IOException e) {
			log.warning("connection did not close properly: "+e.getMessage());
		}
		log.info("endpoint has terminated to: "+getOtherEndpointId());
	}
	
	/**
	 * Start handling a protocol. Only one instance of a protocol can be handled
	 * at a time. Either client or server may start/initiate the use of the protocol.
	 * @see {@link pb.protocols.Protocol}
	 * @param protocol the protocol to handle
	 * @throws ProtocolAlreadyRunning if there is already an instance of this protocol
	 * running on this endpoint
	 */
	public void handleProtocol(Protocol protocol) throws ProtocolAlreadyRunning {
		synchronized(protocols) {
			if(protocols.containsKey(protocol.getProtocolName())){
				throw new ProtocolAlreadyRunning();
			} else {
				protocols.put(protocol.getProtocolName(),protocol);
				log.info("now handling protocol: "+protocol.getProtocolName());
			}
		}
	}
	
	/**
	 * Stop a protocol that is already being handled. It will be removed
	 * from the endpoints set of handled protocols.
	 * @see {@link pb.protocols.Protocol}
	 * @param protocolName the protocol name to stop
	 */
	public void stopProtocol(String protocolName) {
		synchronized(protocols) {
			if(!protocols.containsKey(protocolName)) {
				log.warning("no instance of protocol to stop: "+protocolName);
				return;
			}
			protocols.get(protocolName).stopProtocol();
			protocols.remove(protocolName);
		}
	}
	
	/**
	 * 
	 * @return the id of the other endpoint
	 */
	public String getOtherEndpointId() {
		return socket.getInetAddress().toString()+":"+socket.getPort();
	}

	/**
	 * 
	 * @param string protocol name
	 * @return the protocol with the given name, if it is being handled or null
	 * otherwise
	 */
	public Protocol getProtocol(String string) {
		synchronized(protocols) {
			return protocols.get(string);
		}
	}
}
