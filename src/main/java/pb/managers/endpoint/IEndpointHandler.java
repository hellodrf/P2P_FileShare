package pb.managers.endpoint;

import pb.protocols.Protocol;

public interface IEndpointHandler {
	/**
	 * The endpoint is ready to use.
	 * @param endpoint
	 */
	public void endpointReady(Endpoint endpoint);
	
	/**
	 * The endpoint close() method has been called and completed.
	 * @param endpoint
	 */
	public void endpointClosed(Endpoint endpoint);
	
	/**
	 * The endpoint has abruptly disconnected. It can no longer
	 * send or receive data.
	 * @param endpoint
	 */
	public void endpointDisconnectedAbruptly(Endpoint endpoint);
	
	/**
	 * An invalid message was received over the endpoint.
	 * @param endpoint
	 */
	public void endpointSentInvalidMessage(Endpoint endpoint);
	
	/**
	 * The endpoint has requested a protocol to start. If the protocol
	 * is allowed then the manager should tell the endpoint to handle it
	 * using {@link pb.managers.endpoint.Endpoint#handleProtocol(Protocol)}
	 * before returning true.
	 * @param protocol
	 * @return true if the protocol was started, false if not (not allowed to run)
	 */
	public boolean protocolRequested(Endpoint endpoint, Protocol protocol);
}
