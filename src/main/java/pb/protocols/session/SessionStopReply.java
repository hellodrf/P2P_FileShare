package pb.protocols.session;

import pb.protocols.Document;
import pb.protocols.InvalidMessage;
import pb.protocols.Message;

/**
 * Message sent in reply to a stop request.
 * @see {@link pb.protocols.session.SessionProtocol}
 * @author aaron
 *
 */
public class SessionStopReply extends Message {
	static final public String name = "SessionStopReply";
	
	/**
	 * Initialiser when given message parameters explicitly. Note that
	 * in this message there are no additional parameters.
	 */
	public SessionStopReply() {
		super(name,SessionProtocol.protocolName,Message.Type.Reply);
	}
	
	/**
	 * Initialiser when given message parameters in a doc. Must throw
	 * InvalidMessag if any of the required parameters are not
	 * in the doc, including the appropriate msg parameter.
	 * @param doc with the message details
	 * @throws InvalidMessage when the doc does not contain all of the required parameters
	 */
	public SessionStopReply(Document doc) throws InvalidMessage {
		super(name,SessionProtocol.protocolName,Message.Type.Reply,doc); // really just testing the name, otherwise nothing more to test
		this.doc=doc;
	}
}
