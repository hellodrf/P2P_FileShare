package pb.utils;

import java.util.Timer;
import java.util.TimerTask;

import pb.protocols.ICallback;

/**
 * A singleton class to provide various utility functions. It must always be
 * accessed statically as Utils.getInstance()...
 * 
 * @author aaron
 *
 */
public class Utils {
	private static Utils utils;
	
	/**
	 * Default server port
	 */
	public static final int serverPort = 3100;
	
	/**
	 * Default server host
	 */
	public static final String serverHost = "localhost";
	
	/**
	 * Default index server port
	 */
	public static final int indexServerPort = 3101;
	
	/**
	 * Chunk size in bytes to use when transferring a file
	 */
	public static final int chunkSize = 16*1024;
	
	/**
	 * Use of a single timer object over the entire system helps
	 * to reduce thread usage.
	 */
	private Timer timer = new Timer();
	
	public Utils() {
		timer=new Timer();
	}
	
	public static synchronized Utils getInstance() {
		if(utils==null) utils=new Utils();
		return utils;
	}
	
	/**
	 * Convenience method to set an anonymous method callback
	 * after a timeout delay. Go JavaScript :-)
	 * <br/>
	 * Use this method like: 
	 * <code>
	 * Utils.getInstance().setTimeout(()->{doSomething();},10000);
	 * </code>
	 * @param callback the method to call
	 * @param delay the delay in ms before calling the method
	 */
	public void setTimeout(ICallback callback,long delay) {
		// nicely, this is thread safe
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				callback.callback();
			}
			
		}, delay);
	}
	
	/**
	 * Call before the system exits.
	 */
	public void cleanUp() {
		timer.cancel();
		System.gc(); // need to do this to cleanup timer tasks and allow jvm to quit
	}
}
