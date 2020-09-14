package pb.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import pb.protocols.event.IEventCallback;

/**
 * Simple eventable object. Does not provide for
 * canceling event callbacks.
 * @author aaron
 *
 */
public class Eventable extends Thread {
	private static Logger log = Logger.getLogger(Eventable.class.getName());
	
	/**
	 * Event callbacks
	 */
	private Map<String,List<IEventCallback>> callbacks;
	
	/**
	 * Initializer
	 */
	public Eventable() {
		callbacks=new HashMap<>();
	}
	
	/**
	 * Send event args to all of the callbacks registered
	 * for event name, and to all callbacks registered for special
	 * event "*".
	 * @param eventName event name
	 * @param args event arguments
	 * @return true if at least one callback received the event
	 */
	public synchronized boolean emit(String eventName, Object... args) {
		boolean hit=false;
		if(callbacks.containsKey("*")) {
			callbacks.get("*").forEach((callback)->{
				// TODO: make this little bit of code more efficient
				Object[] newargs=new Object[args.length+1];
				newargs[0]=eventName;
				for(int i=0;i<args.length;i++) newargs[i+1]=args[i];
				callback.callback(newargs);
			});
			hit=true;
		}
		if(localEmit(eventName,args)) hit=true;
		if(!hit)log.warning("no callbacks for event: "+eventName);
		return hit;
	}
	
	/**
	 * Send event args to all of the callbacks registered
	 * for event name.
	 * @param eventName
	 * @param args
	 * @return true if at least one callback received the event
	 */
	public synchronized boolean localEmit(String eventName, Object... args) {
		boolean hit=false;
		if(callbacks.containsKey(eventName)) {
			callbacks.get(eventName).forEach((callback)->{
				callback.callback(args);
			});
			hit=true;
		}
		return hit;
	}
	
	/**
	 * Add a new callback for an event. The special event name "*" is used
	 * for callbacks that want to receive all events.
	 * @param eventName event name
	 * @param callback callback to handle event
	 * @return this event handler for chaining
	 */
	public synchronized Eventable on(String eventName, IEventCallback callback) {
		if(!callbacks.containsKey(eventName)) {
			callbacks.put(eventName,new ArrayList<IEventCallback>());
		}
		callbacks.get(eventName).add(callback);
		return this;
	}
}
