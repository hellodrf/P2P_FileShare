package pb;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import pb.managers.IOThread;
import pb.managers.ServerManager;
import pb.managers.endpoint.Endpoint;
import pb.utils.Utils;

/**
 * Simple index server to discover peers that have files.
 */

public class IndexServer {
	private static final Logger log = Logger.getLogger(IndexServer.class.getName());
	
	/*
	 * Events that this index server will listen to from the client.
	 */
	
	/**
	 * Emitted to request the index to be updated. The argument
	 * must have the format "host:port:filename"
	 * <ul>
	 * <li>{@code args[0] instanceof String}</li>
	 * </ul>
	 */
	public static final String indexUpdate = "INDEX_UPDATE";
	
	/**
	 * Emitted to query the index for keywords. The argument
	 * must have the format "keyword,keyword,..."
	 * <ul>
	 * <li>{@code args[0] instanceof String}</li>
	 * </ul>
	 */
	public static final String queryIndex = "QUERY_INDEX";
	
	/**
	 * Emitted to tell the index server that your peer is
	 * available for other peers to connect to it. The argument
	 * must have the format "host:port"
	 * <ul>
	 * <li>{@code args[0] instanceof String}</li>
	 * </ul>
	 */
	public static final String peerUpdate = "PEER_UPDATE";
	
	/*
	 * Events that this server will send back to the client.
	 */
	
	/**
	 * Emitted to say that the index update failed. The
	 * argument is the update that failed.
	 * <ul>
	 * <li>{@code args[0] instanceof String}</li>
	 * </ul>
	 */
	public static final String indexUpdateError = "INDEX_UPDATE_ERROR";
	
	/**
	 * Emitted as a query response. The argument either gives
	 * a response in the form "host:port:filename" or the empty
	 * string "" to mean no more responses remain.
	 * <ul>
	 * <li>{@code args[0] instanceof String}</li>
	 * </ul>
	 */
	public static final String queryResponse = "QUERY_RESPONSE";
	
	/**
	 * Emitted when the query was in error. No argument is given.
	 */
	public static final String queryError = "QUERY_ERROR";
	
	/**
	 * Storage of the key value index
	 * "filename" to list of "PeerIP:PeerPort" strings that have that file
	 */
	public static final Map<String,Set<String>> keyValueMap=new HashMap<>();
	
	/**
	 * Last time seen "PeerIP:PeerPort" to timestamp, the last time the peer has
	 * been seen. We will use this to give the most recent peer that has the file.
	 */
	public static final Map<String,Long> lastTimeSeen=new HashMap<>();
	
	/**
	 * The default port number for the server.
	 */
	private static int port=Utils.indexServerPort; // default port number for the server
	

	/**
	 * Update the index with the filename and peerPort.
	 * @param filename
	 * @param peerPort
	 */
	private static void indexUpdate(String filename,String peerPort) {
		synchronized(keyValueMap) {
			if(!keyValueMap.containsKey(filename)) {
				keyValueMap.put(filename, new HashSet<String>());
			}
			Set<String> possiblePeers=keyValueMap.get(filename);
			possiblePeers.add(peerPort);
		}
	}
	
	/**
	 * Transmit a response for each hit. Return the peer that has the file
	 * and that was the most recently seen, to try and make sure its still
	 * online.
	 * @param hits
	 * @param client
	 */
	private static void transmitHits(List<String> hits,Endpoint client) {
		if(hits.isEmpty()) {
			log.info("Sending blank query response to signal disconnection");
			client.emit(queryResponse, "");
			return;
		}
		String hit = hits.remove(0);
		synchronized(keyValueMap) {
			synchronized(lastTimeSeen) {
				if(keyValueMap.containsKey(hit)) {
					List<String> peers = new ArrayList<>(keyValueMap.get(hit));
					peers.sort((o1, o2) -> {
						// sort largest to smallest
						return lastTimeSeen.get(o2).compareTo(lastTimeSeen.get(o1));
					});
					log.info("Sending query response: "+peers.get(0)+":"+hit);
					client.emit(queryResponse, peers.get(0)+":"+hit);
				}
			}
		}
		Utils.getInstance().setTimeout(()-> transmitHits(hits,client), 100);
		// transmit 10 hits per second... no real bandwidth control here.
	}
	
	/**
	 * Generate hits and return them to the client. Not a very
	 * efficient search mechanism, but ok for testing.
	 * @param query a comma separated list of terms to search for
	 */
	private static void queryIndex(String query, Endpoint client) {
		String[] terms = query.split(",");
		Set<String> hits = new HashSet<>();
		List<String> filenames;
		synchronized(keyValueMap) {
			filenames=new ArrayList<>(keyValueMap.keySet());
		}

		for(String term : terms) {
			boolean isHit = false;
			for(String filename : filenames) {
				String fileLower = filename.toLowerCase();
				if(fileLower.contains(term.toLowerCase())) {
					hits.add(filename);
					isHit = true;
				}
			}
			if (!isHit) {
				log.info("Cannot find file on index: " + term);
				client.emit(queryError, term);
			}
		}
		transmitHits(new ArrayList<>(hits),client);
	}
	
	/**
	 * Keep a time stamp of the last time we've seen this peer. Multiple
	 * endpoints could call this at the same time.
	 * @param peerPort
	 */
	private static void peerUpdate(String peerPort) {
		synchronized(lastTimeSeen) {
			lastTimeSeen.put(peerPort, Instant.now().toEpochMilli());
		}
	}
	
	private static void help(Options options){
		String header = "PB Index Server\n\n";
		String footer = "\ncontact dsgroup13@unimelb.edu.au for issues.";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("pb.IndexServer", header, options, footer, true);
		System.exit(-1);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
    	// set a nice log format
		System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tl:%1$tM:%1$tS:%1$tL] [%4$s] %2$s: %5$s%n");
        
    	// parse command line options
        Options options = new Options();
        options.addOption("port",true,"server port, an integer");
		options.addOption("password",true,"password, an string");
        
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
			cmd = parser.parse(options, args);
		} catch (ParseException e1) {
			help(options);
		}

		assert cmd != null;
		if(cmd.hasOption("port")) {
        	try {
        		port = Integer.parseInt(cmd.getOptionValue("port"));
			} catch (NumberFormatException e){
				System.out.println("-port requires a port number, parsed: "+cmd.getOptionValue("port"));
				help(options);
			}
        }

		// create a server manager and setup event handlers
		ServerManager serverManager = null;
		if(cmd.hasOption("password")) {
			String password = cmd.getOptionValue("password");
			if (password!=null) {
				serverManager = new ServerManager(port, password);
			}
		} else {
			serverManager = new ServerManager(port);
		}

        // event handlers
        // we must define the event handler callbacks BEFORE starting
        // the server, so that we don't miss any events.
		assert serverManager != null;
		serverManager.on(ServerManager.sessionStarted,(eventArgs)->{
        	Endpoint endpoint = (Endpoint)eventArgs[0];
        	log.info("Client session started: "+endpoint.getOtherEndpointId());
        	endpoint.on(indexUpdate, (eventArgs2)->{
        		String update = (String) eventArgs2[0];
        		log.info("Received index update: "+update);
        		String[] parts=update.split(":",3);
        		if(parts.length!=3) {
        			endpoint.emit(indexUpdateError,update);
        		} else {
	        		String peerPort = parts[0]+":"+parts[1];
	        		indexUpdate(parts[2],peerPort);
					peerUpdate(peerPort);
        		}
        	}).on(queryIndex, (eventArgs2)->{
        		String query = (String) eventArgs2[0];
        		log.info("Received query: "+query);
        		queryIndex(query,endpoint);
        	}).on(peerUpdate, (eventArgs2)->{
        		String peerport = (String) eventArgs2[0];
        		log.info("Received peer update: "+peerport);
        		peerUpdate(peerport);
        	});
        }).on(ServerManager.sessionStopped,(eventArgs)->{
        	Endpoint endpoint = (Endpoint)eventArgs[0];
        	log.info("Client session ended: "+endpoint.getOtherEndpointId());
        }).on(ServerManager.sessionError, (eventArgs)->{
        	Endpoint endpoint = (Endpoint)eventArgs[0];
        	log.warning("Client session ended in error: "+endpoint.getOtherEndpointId());
        }).on(IOThread.ioThread, (eventArgs)->{
        	String peerport = (String) eventArgs[0];
        	// we don't need this info, but let's log it
        	log.info("Listening on Internet address: "+peerport);
        });
        
        // start up the server
        log.info("PB Index Server starting up");
        serverManager.start();

        serverManager.join();
		Utils.getInstance().cleanUp();
    }

}
