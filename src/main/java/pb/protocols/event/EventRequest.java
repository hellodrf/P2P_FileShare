package pb.protocols.event;

import pb.protocols.Document;
import pb.protocols.InvalidMessage;
import pb.protocols.Message;

public class EventRequest extends Message {
	static final public String name = "EventRequest";
	
	public EventRequest(String eventName, String eventData) {
		super(name, EventProtocol.protocolName, Message.Type.Request);
		doc.append("eventName", eventName);
		doc.append("eventData", eventData);
	}

	public EventRequest(Document doc) throws InvalidMessage {
		super(name,EventProtocol.protocolName,Message.Type.Request,doc);
		Message.validateStringType("eventName", doc);
		Message.validateStringType("eventData", doc);
		this.doc=doc;
	}
	
	public String getEventName() {
		return doc.getString("eventName");
	}
	
	public String getEventData() {
		return doc.getString("eventData");
	}
}
