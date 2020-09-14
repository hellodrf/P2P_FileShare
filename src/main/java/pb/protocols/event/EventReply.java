package pb.protocols.event;

import pb.protocols.Document;
import pb.protocols.InvalidMessage;
import pb.protocols.Message;

public class EventReply extends Message {
	static final public String name = "EventReply";
	
	public EventReply() {
		super(name, EventProtocol.protocolName, Message.Type.Reply);
	}

	public EventReply(Document doc) throws InvalidMessage {
		super(name,EventProtocol.protocolName,Message.Type.Reply,doc); // really just testing the name, otherwise nothing more to test
		this.doc=doc;
	}
}
