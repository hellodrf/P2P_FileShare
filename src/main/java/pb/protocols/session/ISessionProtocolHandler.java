package pb.protocols.session;

import pb.managers.endpoint.Endpoint;

public interface ISessionProtocolHandler {
	/**
	 * The session has started
	 * @param endpoint
	 */
	public void sessionStarted(Endpoint endpoint);
	
	/**
	 * The session has been stopped.
	 * @param endpoint
	 */
	public void sessionStopped(Endpoint endpoint);
}
