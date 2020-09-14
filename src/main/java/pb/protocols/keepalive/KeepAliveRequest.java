package pb.protocols.keepalive;

import pb.protocols.Document;
import pb.protocols.InvalidMessage;
import pb.protocols.Message;

/**
 * Request message for the KeepAlive protocol.
 * @see {@link pb.protocols.keepalive.KeepAliveProtocol}
 * @author aaron
 *
 */
public class KeepAliveRequest extends Message {
	static final public String name = "KeepAliveRequest";
	
	/**
	 * Initialiser when given message parameters explicitly. Note that
	 * in this message there are no additional parameters.
	 */
	public KeepAliveRequest() {
		super(name,KeepAliveProtocol.protocolName,Message.Type.Request);
	}
	
	/**
	 * Initialiser when given message parameters in a doc. Must throw
	 * InvalidMessage if any of the required parameters are not
	 * in the doc, including the appropriate msg parameter.
	 * @param doc with the message details
	 * @throws InvalidMessage when the doc does not contain all of the required parameters
	 */
	public KeepAliveRequest(Document doc) throws InvalidMessage {
		super(name,KeepAliveProtocol.protocolName,Message.Type.Request,doc); // really just testing the name, otherwise nothing more to test
		this.doc=doc;
	}
	
}
