package pb.protocols;


/**
 * Request/reply protocol objects must implement this interface.
 * 
 * @see #startAsClient()
 * @see #startAsServer()
 * @see #sendRequest(Message)
 * @see #receiveReply(Message)
 * @see #receiveRequest(Message)
 * @see #sendReply(Message)
 * @see pb.protocols.Message
 *
 */
public interface IRequestReplyProtocol {
	/**
	 * Start the protocol as a client.
	 */
	public void startAsClient();
	
	/**
	 * Start the protocol as a server.
	 */
	public void startAsServer();
	
	/**
	 * Send a request message.
	 * @param msg
	 */
	public void sendRequest(Message msg);
	
	/**
	 * Receive a reply message.
	 * @param msg
	 */
	public void receiveReply(Message msg);
	
	/**
	 * Receive a request message.
	 * @param msg
	 */
	public void receiveRequest(Message msg);
	
	/**
	 * Send a reply message.
	 * @param msg
	 */
	public void sendReply(Message msg);
}
