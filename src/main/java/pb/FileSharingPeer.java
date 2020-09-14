package pb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Base64;

import pb.managers.ClientManager;
import pb.managers.IOThread;
import pb.managers.PeerManager;
import pb.managers.ServerManager;
import pb.managers.endpoint.Endpoint;
import pb.utils.Utils;

/**
 * TODO: for Project 2B The FileSharingPeer is a simple example of using a
 * PeerManager to control both a server and any number of client connections to
 * a server/peers. <br/>
 * <b>Please see the video recording on Canvas for an explanation of what the
 * the FileSharingPeer is meant to do.</b>
 * 
 * @author aaron
 *
 */
public class FileSharingPeer {
	private static Logger log = Logger.getLogger(FileSharingPeer.class.getName());

	/**
	 * Events that the peers use between themselves.
	 */

	/**
	 * Emitted when a peer wants to get a file from another peer. The single
	 * argument is a string that is the filename to get.
	 * <ul>
	 * <li>{@code args[0] instanceof String}
	 * </ul>
	 */
	private static final String getFile = "GET_FILE";

	/**
	 * Emitted when a peer is sending a chunk of a file to another peer. The single
	 * argument is a string that is a Base64 encoded byte array that represents the
	 * chunk of the file. If the argument is the empty string "" then it indicates
	 * there are no more chunks to receive.
	 * <ul>
	 * <li>{@code args[0] instanceof String}
	 * </ul>
	 */
	private static final String fileContents = "FILE_CONTENTS";

	/**
	 * Emitted when a file does not exist or chunks fail to be read. The receiving
	 * peer should then abandon waiting to receive the rest of the chunks of the
	 * file. There are no arguments.
	 */
	private static final String fileError = "FILE_ERROR";

	/**
	 * port to use for this peer's server
	 */
	private static int peerPort = Utils.serverPort; // default port number for this peer's server

	/**
	 * port to use when contacting the index server
	 */
	private static int indexServerPort = Utils.indexServerPort; // default port number for index server

	/**
	 * host to use when contacting the index server
	 */
	private static String host = Utils.serverHost; // default host for the index server

	/**
	 * chunk size to use (bytes) when transferring a file
	 */
	private static int chunkSize = Utils.chunkSize;

	/**
	 * buffer for file reading
	 */
	private static byte[] buffer = new byte[chunkSize];

	/**
	 * Read up to chunkSize bytes of a file and send to client. If we have not
	 * reached the end of the file then set a timeout to read some more bytes. Since
	 * this is using the timer thread we have the danger that the transmission will
	 * block and that this will block all the other timeouts. We could either use
	 * another thread for each file transfer or else allow for buffering of outgoing
	 * messages at the endpoint, to overcome this issue.
	 * 
	 * @param in       the file input stream
	 * @param endpoint the endpoint to send the file
	 */
	public static void continueTransmittingFile(InputStream in, Endpoint endpoint) {
		try {
			int read = in.read(buffer);
			if (read == -1) {
				endpoint.emit(fileContents, ""); // signals no more bytes in file
				in.close();
			} else {
				endpoint.emit(fileContents, new String(Base64.encodeBase64(Arrays.copyOfRange(buffer, 0, read)),
						StandardCharsets.US_ASCII));
				if (read < chunkSize) {
					endpoint.emit(fileContents, "");
					in.close();
				} else {
					Utils.getInstance().setTimeout(() -> {
						continueTransmittingFile(in, endpoint);
					}, 100); // limit throughput to about 160kB/s, hopefully your bandwidth can keep up :-)
				}
			}
		} catch (IOException e) {
			endpoint.emit(fileError);
		}
	}

	/**
	 * Test for the file existence and then start transmitting it. Emit
	 * {@link #fileError} if file can't be accessed.
	 * 
	 * @param filename
	 * @param endpoint
	 */
	public static void startTransmittingFile(String filename, Endpoint endpoint) {
		try {
			InputStream in = new FileInputStream(filename);
			continueTransmittingFile(in, endpoint);
		} catch (FileNotFoundException e) {
			endpoint.emit(fileError);
		}
	}

	/**
	 * Emit a filename as an index update if possible, close when all done.
	 * 
	 * @param filenames
	 * @param endpoint
	 */
	public static void emitIndexUpdate(String peerport, List<String> filenames, Endpoint endpoint,
			ClientManager clientManager) {
		if (filenames.size() == 0) {
			clientManager.shutdown(); // no more index updates to do
		} else {
			String filename = filenames.remove(0);
			log.info("Sending index update: " + peerport + ":" + filename);
			// an index update has the format: host:port:filename
			endpoint.emit(IndexServer.indexUpdate, peerport + ":" + filename);
			Utils.getInstance().setTimeout(() -> {
				emitIndexUpdate(peerport, filenames, endpoint, clientManager);
			}, 100); // send 10 index updates per second, this shouldn't kill the bandwidth :-]
		}
	}

	/**
	 * Open a client connection to the index server and send the filenames to update
	 * the index.
	 * 
	 * @param filenames
	 * @param peerManager
	 * @throws InterruptedException
	 * @throws UnknownHostException
	 */
	public static void uploadFileList(List<String> filenames, PeerManager peerManager, String peerport)
			throws UnknownHostException, InterruptedException {
		// connect to the index server and tell it the files we are sharing
		ClientManager clientManager = peerManager.connect(indexServerPort, host);

		/*
		 * TODO for project 2B. Listen for peerStarted, peerUpdate, peerStopped on the
		 * clientManager. Further listen on the endpoint when available for
		 * indexUpdateError events. Print out any index update errors that occur. Use
		 * emitIndexUpdate(...) to send the index updates. Print out something
		 * informative for the events when they they occur.
		 */

		clientManager.start();
	}

	/**
	 * Share files by starting up a server manager and then sending updates to the
	 * index server to say which files are being shared.
	 * 
	 * @param files list of file names to share
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private static void shareFiles(String[] files) throws InterruptedException, IOException {
		List<String> filenames = new ArrayList<String>();
		for (String file : files) {
			filenames.add(file);
		}
		PeerManager peerManager = new PeerManager(peerPort);

		/*
		 * TODO for project 2B. Listen for peerStarted, peerStopped, peerError and
		 * peerServerManager on the peerManager. Listen for getFile events on the
		 * endpoint when available and use startTransmittingFile(...) to handle such
		 * events. Start uploading the file names that are being shared on when the
		 * peerServerManager is ready and when the ioThread event has been received.
		 * Print out something informative for the events when they occur.
		 */

		peerManager.start();

		// just keep sharing until the user presses "return"
		BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Press RETURN to stop sharing");
		input.readLine();
		System.out.println("RETURN pressed, stopping the peer");
		peerManager.shutdown();
	}

	/**
	 * Process a query response from the index server and download the file
	 * 
	 * @param queryResponse
	 * @throws InterruptedException
	 */
	private static void getFileFromPeer(PeerManager peerManager, String response) throws InterruptedException {
		// Create a independent client manager (thread) for each download
		// response has the format: PeerIP:PeerPort:filename
		String[] parts = response.split(":", 3);
		ClientManager clientManager = null;

		/*
		 * TODO for project 2B. Check that the individual parts returned from the server
		 * have the correct format and that we make a connection to the peer. Print out
		 * any errors and just return in this case. Otherwise you have a clientManager
		 * that has connected.
		 */

		try {
			OutputStream out = new FileOutputStream(parts[2]);

			/*
			 * TODO for project 2B. listen for peerStarted, peerStopped and peerError events
			 * on the clientManager. Listen for fileContents and fileError events on the
			 * endpoint when available and handle them appropriately. Handle error
			 * conditions by printing something informative and shutting down the
			 * connection. Remember to emit a getFile event to request the file form the
			 * peer. Print out something informative for the events that occur.
			 */

			clientManager.start();
			/*
			 * we will join with this thread later to make sure that it has finished
			 * downloading before the jvm quits.
			 */			
		} catch (FileNotFoundException e) {
			System.out.println("Could not create file: " + parts[2]);
		}

	}

	/**
	 * Query the index server for the keywords and download files for each of the
	 * query responses.
	 * 
	 * @param keywords list of keywords to query for and download matching files
	 * @throws InterruptedException
	 * @throws UnknownHostException
	 */
	private static void queryFiles(String[] keywords) throws UnknownHostException, InterruptedException {
		String query = String.join(",", keywords);
		// connect to the index server and tell it the files we are sharing
		PeerManager peerManager = new PeerManager(peerPort);
		ClientManager clientManager = peerManager.connect(indexServerPort, host);

		/*
		 * TODO for project 2B. Listen for peerStarted, peerStopped and peerError events
		 * on the clientManager. Listen for queryResponse and queryError events on the
		 * endpoint when it is available, and handle them appropriately. Simply shutdown
		 * the connection if errors occur. Use getFileFromPeer(...) when query responses
		 * arrive. Make sure to emit your queryIndex event to actually query the index
		 * server. Print out something informative for the events that occur.
		 */

		clientManager.start();
		clientManager.join(); // wait for the query to finish, since we are in main
		/*
		 * We also have to join with any other client managers that were started for
		 * download purposes.
		 */
		peerManager.joinWithClientManagers();
	}

	private static void help(Options options) {
		String header = "PB Peer for Unimelb COMP90015\n\n";
		String footer = "\ncontact aharwood@unimelb.edu.au for issues.";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("pb.Peer", header, options, footer, true);
		System.exit(-1);
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		// set a nice log format
		System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tl:%1$tM:%1$tS:%1$tL] [%4$s] %2$s: %5$s%n");

		// parse command line options
		Options options = new Options();
		options.addOption("port", true, "peer server port, an integer");
		options.addOption("host", true, "index server hostname, a string");
		options.addOption("indexServerPort", true, "index server port, an integer");
		Option optionShare = new Option("share", true, "list of files to share");
		optionShare.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(optionShare);
		Option optionQuery = new Option("query", true, "keywords to search for and download files that match");
		optionQuery.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(optionQuery);

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e1) {
			help(options);
		}

		if (cmd.hasOption("port")) {
			try {
				peerPort = Integer.parseInt(cmd.getOptionValue("port"));
			} catch (NumberFormatException e) {
				System.out.println("-port requires a port number, parsed: " + cmd.getOptionValue("port"));
				help(options);
			}
		}

		if (cmd.hasOption("indexServerPort")) {
			try {
				indexServerPort = Integer.parseInt(cmd.getOptionValue("indexServerPort"));
			} catch (NumberFormatException e) {
				System.out.println(
						"-indexServerPort requires a port number, parsed: " + cmd.getOptionValue("indexServerPort"));
				help(options);
			}
		}

		if (cmd.hasOption("host")) {
			host = cmd.getOptionValue("host");
		}

		// start up the client
		log.info("PB Peer starting up");

		if (cmd.hasOption("share")) {
			String[] files = cmd.getOptionValues("share");
			shareFiles(files);
		} else if (cmd.hasOption("query")) {
			String[] keywords = cmd.getOptionValues("query");
			queryFiles(keywords);
		} else {
			System.out.println("must use either the -query or -share option");
			help(options);
		}
		Utils.getInstance().cleanUp();
		log.info("PB Peer stopped");
	}

}
