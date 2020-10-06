package pb;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import pb.managers.ClientManager;
import pb.managers.ServerManager;
import pb.managers.endpoint.Endpoint;
import pb.utils.Utils;

public class AdminClient  {
	private static final Logger log = Logger.getLogger(AdminClient.class.getName());
	private static int port=Utils.serverPort; // default port number for the server
	private static String host=Utils.serverHost; // default host for the server

	private static void help(Options options){
		String header = "PB Admin Client\n\n";
		String footer = "\ncontact dsgroup13@unimelb.edu.au for issues.";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("pb.Client", header, options, footer, true);
		System.exit(-1);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException
    {
    	// set a nice log format
		System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tl:%1$tM:%1$tS:%1$tL] %2$s %4$s: %5$s%n");
        
    	// parse command line options
        Options options = new Options();
        options.addOption("port",true,"server port, an integer");
        options.addOption("host",true,"hostname, a string");
		options.addOption("password",true,"password, a string");
		options.addOption("shutdown",false,"tell server to shutdown");
		options.addOption("force",false,"tell server to shutdown (force)");
		options.addOption("vader",false,"tell server to shutdown (vader)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
			cmd = parser.parse(options, args);
		} catch (ParseException e1) {
			help(options);
		}

		String password = "";
		assert cmd != null;
		if(cmd.hasOption("password")) {
			password = cmd.getOptionValue("password");
			if (password == null) {
				System.out.println("-password requires a string as password.");
				help(options);
			}
		}

        if(cmd.hasOption("port")){
        	try{
				port = Integer.parseInt(cmd.getOptionValue("port"));
			} catch (NumberFormatException e){
				System.out.println("-port requires a port number, parsed: "+cmd.getOptionValue("port"));
				help(options);
			}
        }
        
        if(cmd.hasOption("host")) {
        	host = cmd.getOptionValue("host");
        }
        
        // start up the client
        log.info("PB Client starting up");
        
        // the client manager will make a connection with the server
        // and the connection will use a thread that prevents the JVM
        // from terminating immediately
        ClientManager clientManager = new ClientManager(host,port);
        clientManager.start();

		CommandLine finalCmd = cmd;
		String finalPassword = password;
		clientManager.on(ClientManager.sessionStarted, (args1) -> {
			Endpoint endpoint = (Endpoint) args1[0];
			if (finalCmd.hasOption("shutdown")) {
				if (finalCmd.hasOption("force")) {
					endpoint.emit(ServerManager.forceShutdownServer, finalPassword);
					log.info("Shutdown directive sent, finishing");
				}
				else if (finalCmd.hasOption("vader")) {
					endpoint.emit(ServerManager.vaderShutdownServer, finalPassword);
					log.info("Shutdown directive sent, finishing");
				} else {
					endpoint.emit(ServerManager.shutdownServer, finalPassword);
					log.info("Shutdown directive sent, finishing");
				}
			}
			// give server time to shutdown
			try {
				log.info("Wait 3 seconds for server to finish up");
				Thread.sleep(3000);
			} catch (InterruptedException ignored) {}
			log.info("Time is up, exiting now");
			endpoint.close();
		});

        clientManager.join();
        Utils.getInstance().cleanUp();
    }
}
