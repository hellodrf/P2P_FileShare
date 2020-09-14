package pb.protocols.event;

@FunctionalInterface
public interface IEventCallback {
	/**
	 * Handle events with variable arguments
	 * @param args
	 */
	public void callback(Object... args);
}
