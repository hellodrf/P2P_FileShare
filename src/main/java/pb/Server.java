package pb;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import pb.managers.ServerManager;
import pb.utils.Utils;

/**
 * Server main. Parse command line options and provide default values.
 * 
 * @see {@link pb.managers.ServerManager}
 * @see {@link pb.utils.Utils}
 * @author aaron
 *
 */
public class Server {
	private static Logger log = Logger.getLogger(Server.class.getName());
	private static int port=Utils.serverPort; // default port number for the server
	

	private static void help(Options options){
		String header = "PB Server for Unimelb COMP90015\n\n";
		String footer = "\ncontact aharwood@unimelb.edu.au for issues.";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("pb.Server", header, options, footer, true);
		System.exit(-1);
	}
	
	public static void main( String[] args ) throws IOException
    {
    	// set a nice log format
		System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tl:%1$tM:%1$tS:%1$tL] %2$s %4$s: %5$s%n");
        
    	// parse command line options
        Options options = new Options();
        options.addOption("port",true,"server port, an integer");
        
       
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
			cmd = parser.parse( options, args);
		} catch (ParseException e1) {
			help(options);
		}
        
        if(cmd.hasOption("port")){
        	try{
        		port = Integer.parseInt(cmd.getOptionValue("port"));
			} catch (NumberFormatException e){
				System.out.println("-port requires a port number, parsed: "+cmd.getOptionValue("port"));
				help(options);
			}
        }
        
        
        // start up the server
        log.info("PB Server starting up");
        
        // the server manager will start an io thread and this will prevent
        // the JVM from terminating
        ServerManager serverManager = new ServerManager(port);
        serverManager.start();
        // The simple server does not do any application logic, but will
        // (when you have implemented it in the ServerManager class)
        // just continue to run until it is terminated by ctrl+C, is killed
        // by some other OS signal, or an "admin" client connects and sends 
        // a "SERVER_SHUTDOWN" or "SERVER_FORCE_SHUTDOWN" or if really needed ...
        // "SERVER_VADER_SHUTDOWN" event to the server, over the event protocol. 
        // See AdminClient.java for more info on what is expected.
        
        // the very last thing to do
        Utils.getInstance().cleanUp();
        
    }
}
