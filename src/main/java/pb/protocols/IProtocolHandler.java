package pb.protocols;

import pb.managers.endpoint.Endpoint;

public interface IProtocolHandler {
	/**
	 * The protocol on the endpoint has been violated.
	 * @param endpoint
	 */
	public void protocolViolation(Endpoint endpoint,Protocol protocol);
	
	/**
	 * The protocol on the endpoint is not responding.
	 * @param endpoint
	 */
	public void endpointTimedOut(Endpoint endpoint,Protocol protocol);
	
}
