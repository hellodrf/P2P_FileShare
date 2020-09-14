package pb.protocols;

import pb.protocols.event.EventReply;
import pb.protocols.event.EventRequest;
import pb.protocols.keepalive.KeepAliveReply;
import pb.protocols.keepalive.KeepAliveRequest;
import pb.protocols.session.SessionStartReply;
import pb.protocols.session.SessionStartRequest;
import pb.protocols.session.SessionStopReply;
import pb.protocols.session.SessionStopRequest;

/**
 * Message super class and factory for all protocol messages, to parse a
 * received UTF-8 line of text in JSON format, as an object that represents the
 * message.
 * 
 * @see {@link pb.protocols.Protocol}
 * @author aaron
 *
 */
public class Message {
	/**
	 * Messages are either a request or a reply.
	 */
	static public enum Type {
		Request,
		Reply
	}
	
	/**
	 * All of the message parameters are wrapped up in a Document class.
	 */
	protected Document doc;
	
	/**
	 * Initialiser when given parameters explicitly.
	 * @param name the name of the message (its classname by convention)
	 * @param protocolName the name of the protocol the message belongs to
	 * @param type whether its a Request or a Reply message
	 */
	public Message(String name, String protocolName, Message.Type type) {
		doc = new Document();
		doc.append("name", name);
		doc.append("protocolName", protocolName);
		doc.append("type", type.toString());
	}
	
	static public void validateStringValue(String key,String val,Document doc) throws InvalidMessage {
		if(!doc.containsKey(key)) throw new InvalidMessage();
		if(!(doc.get(key) instanceof String)) throw new InvalidMessage();
		String msg = doc.getString(key);
		if(!msg.equals(val)) throw new InvalidMessage();
	}
	
	static public void validateLongType(String key,Document doc) throws InvalidMessage {
		if(!doc.containsKey(key)) throw new InvalidMessage();
		if(!(doc.get(key) instanceof Long)) throw new InvalidMessage();
	}
	
	static public void validateStringType(String key,Document doc) throws InvalidMessage {
		if(!doc.containsKey(key)) throw new InvalidMessage();
		if(!(doc.get(key) instanceof String)) throw new InvalidMessage();
	}
	
	/**
	 * Initialiser when given parameters in a doc.
	 * @param name the name of the message that is being initialised
	 * @param doc with the message details
	 * @throws InvalidMessage when the name of the message in the doc is incorrect
	 */
	public Message(String name, String protocolName, 
			Message.Type type, Document doc) throws InvalidMessage {
		validateStringValue("name",name,doc);
		validateStringValue("protocolName",protocolName,doc);
		validateStringValue("type",type.toString(),doc);
		if(doc.containsKey("timeoutId")) validateLongType("timeoutId",doc);
	}
	
	/**
	 * Turn a json string into an appropriate message object.
	 * @param json the string to parse, must be in JSON format
	 * @return the appropriate message object
	 * @throws InvalidMessage if no message object matches the message
	 */
	static public Message toMessage(String json) throws InvalidMessage {
		Document doc = Document.parse(json);
		// the following test is somewhat repetitive, but it avoids having
		// to test each message type, handling exceptions for those that are
		// not the matching message type
		if(!doc.containsKey("name")) throw new InvalidMessage();
		if(!(doc.get("name") instanceof String)) throw new InvalidMessage();
		String msg = doc.getString("name");
		switch(msg) {
			case KeepAliveRequest.name: return new KeepAliveRequest(doc);
			case KeepAliveReply.name: return new KeepAliveReply(doc);
			case SessionStartRequest.name: return new SessionStartRequest(doc);
			case SessionStartReply.name: return new SessionStartReply(doc);
			case SessionStopRequest.name: return new SessionStopRequest(doc);
			case SessionStopReply.name: return new SessionStopReply(doc);
			case EventRequest.name: return new EventRequest(doc);
			case EventReply.name: return new EventReply(doc);
			// put more message cases here
			
			// if nothing matches, its invalid
			default: throw new InvalidMessage();
		}
	}
	
	/**
	 * Convert the message to a string for transmission.
	 * @return
	 */
	public String toJsonString() {
		return doc.toJson();
	}
	
	/**
	 * Return the protocol name
	 * @return
	 */
	public String getProtocolName() {
		return doc.getString("protocolName");
	}
	
	/**
	 * Return the message name
	 * @return
	 */
	public String getName() {
		return doc.getString("name");
	}
	
	/**
	 * Return the message type
	 * @return
	 */
	public final Message.Type getType() {
		return Message.Type.valueOf(doc.getString("type"));
	}
	
	/**
	 * Set a timeout id
	 * @param id
	 */
	public final void setTimeoutId(long id) {
		doc.append("timeoutId", id);
	}
	
	/**
	 * Get timeout id
	 * @return id
	 */
	public final long getTimeoutId() {
		if(doc.containsKey("timeoutId"))
			return doc.getLong("timeoutId");
		else return 0;
	}
}
