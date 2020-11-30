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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
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

public class FileSharingPeer {
	private static final Logger log = Logger.getLogger(FileSharingPeer.class.getName());

	/*
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
	private static final int chunkSize = Utils.chunkSize;

	/**
	 * buffer for file reading
	 */
	private static final byte[] buffer = new byte[chunkSize];

	/**
	 * number of remaining files to share/query
	 */
	private static final AtomicInteger fileCount = new AtomicInteger(0);

	private static final Set<String> fileFailed = new HashSet<>();

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
					Utils.getInstance().setTimeout(() -> continueTransmittingFile(in, endpoint), 1);
					// throughput about 16*1,000 = 16,000kB/s, hopefully your bandwidth can keep up :-)
				}
			}
		} catch (IOException e) {
			endpoint.emit(fileError, "IO Error");
		}
	}

	/**
	 * Test for the file existence and then start transmitting it. Emit
	 * {@link #fileError} if file can't be accessed.
	 * 
	 * @param filename to be sent
	 * @param endpoint current endpoint
	 */
	public static void startTransmittingFile(String filename, Endpoint endpoint) {
		try {
			InputStream in = new FileInputStream(filename);
			continueTransmittingFile(in, endpoint);
		} catch (FileNotFoundException e) {
			endpoint.emit(fileError, "File not found");
		}
	}

	/**
	 * Emit a filename as an index update if possible, close when all done.
	 * 
	 * @param filenames to be sent to client
	 * @param endpoint current endpoint
	 */
	public static void emitIndexUpdate(String peerPort, List<String> filenames, Endpoint endpoint,
			ClientManager clientManager) {
		if (filenames.size() == 0) {
			clientManager.shutdown(); // no more index updates to do
		} else {
			String filename = filenames.remove(0);
			log.info("Sending index update: " + peerPort + ":" + filename);
			// an index update has the format: host:port:filename
			endpoint.emit(IndexServer.indexUpdate, peerPort + ":" + filename);
			Utils.getInstance().setTimeout(() ->
					emitIndexUpdate(peerPort, filenames, endpoint, clientManager), 100);
			// send 10 index updates per second, this shouldn't kill the bandwidth :-]
		}
	}

	/**
	 * Open a client connection to the index server and send the filenames to update
	 * the index.
	 * 
	 * @param filenames to be sent to index server
	 * @param peerManager current peerManager
	 * @throws InterruptedException peerManager.connect
	 * @throws UnknownHostException peerManager.connect
	 */
	public static void uploadFileList(List<String> filenames, PeerManager peerManager, String peerPort)
			throws UnknownHostException, InterruptedException {
		// connect to the index server and tell it the files we are sharing
		ClientManager clientManager = peerManager.connect(indexServerPort, host);

		clientManager.on(PeerManager.peerStarted, (args) -> {
			Endpoint endpoint = (Endpoint) args[0];
			log.info("IndexServer session started: " + endpoint.getOtherEndpointId());
			endpoint.on(IndexServer.indexUpdateError, (args1 ->
					log.warning("An error occurred while updating index: " + args1[0])));
			// push index update
			emitIndexUpdate(peerPort, filenames, endpoint, clientManager);
		}).on(PeerManager.peerError, (args) -> {
			Endpoint endpoint = (Endpoint) args[0];
			log.warning("IndexServer session ended in error: " + endpoint.getOtherEndpointId());
		}).on(PeerManager.peerStopped, (args) -> {
			Endpoint endpoint = (Endpoint) args[0];
			log.info("IndexServer session ended: " + endpoint.getOtherEndpointId());
		});
		clientManager.start();
	}

	/**
	 * Share files by starting up a server manager and then sending updates to the
	 * index server to say which files are being shared.
	 * 
	 * @param files list of file names to share
	 * @throws IOException input.readLine()
	 */
	private static void shareFiles(String[] files) throws IOException {
		List<String> filenames = new ArrayList<>(Arrays.asList(files));
		PeerManager peerManager = new PeerManager(peerPort);

		peerManager.on(PeerManager.peerStarted, (args)-> {
			Endpoint endpoint = (Endpoint) args[0];
			log.info("Peer session started: " + endpoint.getOtherEndpointId());
			// listen for fileError & getFile
			endpoint.on(fileError, (args1 ->
					log.warning("File error - " + endpoint.getOtherEndpointId() + ": " + args1[0]))
			).on(getFile, (args2 -> {
				String file = (String)args2[0];
				log.info("Starting transmission:" + file);
				startTransmittingFile(file, endpoint);
			}));
		}).on(PeerManager.peerStopped, (args)-> {
			Endpoint endpoint = (Endpoint) args[0];
			log.info("Peer session ended: " + endpoint.getOtherEndpointId());
		}).on(PeerManager.peerError, (args)-> {
			Endpoint endpoint = (Endpoint) args[0];
			log.severe("IndexServer session ended in error: " + endpoint.getOtherEndpointId());
		}).on(PeerManager.peerServerManager, (args) -> {
			ServerManager serverManager = (ServerManager) args[0];
			// listen for ioThread
			serverManager.on(IOThread.ioThread, (args1)->{
				String peerPort = (String) args1[0];
				log.info("Listening on Internet address: " + peerPort);
				try {
					uploadFileList(filenames, peerManager, peerPort);
				} catch (UnknownHostException e) {
					log.severe("Could not connect to index server");
				} catch (InterruptedException e) {
					log.warning("IndexServer session ended abruptly");
				}
			});
		});
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
	 * @throws InterruptedException peerManager.connect
	 */
	private static void getFileFromPeer(PeerManager peerManager, String response) throws InterruptedException {
		// Create a independent client manager (thread) for each download
		// response has the format: PeerIP:PeerPort:filename
		String[] parts = response.split(":", 3);
		ClientManager clientManager;

		String ipv4_regex = "^((0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)\\.){3}" +
				"(0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)$";

		String port_regex = "([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}" +
				"|65[0-4][0-9]{2}|655[0-3][0-9]|6553[0-5])";


		if (parts[0].matches(ipv4_regex) && parts[1].matches(port_regex)) {
			try {
				clientManager = peerManager.connect(Integer.parseInt(parts[1]), parts[0]);
			} catch (UnknownHostException e) {
				 log.severe("Could not connect to peer: " + parts[0] + ":" + parts[1]);
				synchronized (fileFailed) {
					fileFailed.add(parts[2]);
				}
				return;
			}
		} else {
			log.severe("Illegal peer address: " + Arrays.toString(parts));
			synchronized (fileFailed) {
				fileFailed.add(parts[2]);
			}
			return;
		}

		try {
			//OutputStream out = new FileOutputStream(parts[2]);
			// Debug only: avoid filename clashing
			OutputStream out = new FileOutputStream(java.time.LocalDateTime.now().toString() + "_" + parts[2]);

			clientManager.on(PeerManager.peerStarted, (args) -> {
				Endpoint endpoint = (Endpoint) args[0];
				log.info("Peer session started: " + endpoint.getOtherEndpointId());
				// listen for fileError & fileContents
				endpoint.on(fileError, (args1) -> {
					synchronized (fileFailed) {
						fileFailed.add(parts[2]);
					}
					log.severe("An error occurred while fetching file: " + parts[2]);
					System.out.println("ERROR: Failed to download file: "+ parts[2]);
				}).on(fileContents, (args1) -> {
					try {
						if (args1[0] instanceof String && args1[0].equals("")) {
							out.flush();
							out.close();
							log.info("File download complete: " + parts[2]);
							System.out.println("File downloaded: " + parts[2]);
							clientManager.shutdown();
							if (fileCount.decrementAndGet() == 0) {
								log.info("All files received, shutting down");
								System.out.println("All file(s) received! Shutting down now");
								endpoint.close();
							}
						} else {
							if (args1[0] instanceof String) {
								out.write(Base64.decodeBase64((String) args1[0]));
								log.info("Data received for file: " + parts[2]);
							}
						}
					} catch (IOException e) {
						synchronized (fileFailed) {
							fileFailed.add(parts[2]);
						}
						System.out.println("Could not save file: " + parts[2]);
						endpoint.close();
					}
				});
				log.info("Request emitted to " + endpoint.getOtherEndpointId());
				endpoint.emit(getFile, parts[2]);
				System.out.println("Now downloading file: " + parts[2] + " from " + endpoint.getOtherEndpointId());
			}).on(PeerManager.peerStopped, (args) -> {
				Endpoint endpoint = (Endpoint) args[0];
				log.info("Peer session ended: " + endpoint.getOtherEndpointId());

			}).on(PeerManager.peerError, (args) -> {
				Endpoint endpoint = (Endpoint) args[0];
				log.severe("Peer session ended in error: " + endpoint.getOtherEndpointId());
				endpoint.close();
			});

			clientManager.start();
			/*
			 * we will join with this thread later to make sure that it has finished
			 * downloading before the jvm quits.
			 */			
		} catch (FileNotFoundException e) {
			System.out.println("ERROR: Could not create file: " + parts[2]);
		}
	}

	/**
	 * Query the index server for the keywords and download files for each of the
	 * query responses.
	 * 
	 * @param keywords list of keywords to query for and download matching files
	 * @throws InterruptedException peerManager.connect
	 * @throws UnknownHostException peerManager.connect
	 */
	private static void queryFiles(String[] keywords) throws UnknownHostException, InterruptedException {
		fileCount.set(keywords.length);
		String query = String.join(",", keywords);
		// connect to the index server and tell it the files we are sharing
		PeerManager peerManager = new PeerManager(peerPort);
		ClientManager clientManager = peerManager.connect(indexServerPort, host);

		clientManager.on(PeerManager.peerStarted, (args) -> {
			Endpoint endpoint = (Endpoint) args[0];
			endpoint.on(IndexServer.queryResponse, (args1) -> {
				try {
					if (args1[0].equals("")) {
						log.info("Received blank response from index server, shutting down connection");
						clientManager.shutdown();
					} else {
						log.info("Received response from index server: " + args1[0]);
						log.info("Querying file from " + args1[0]);
						getFileFromPeer(peerManager, (String)args1[0]);
					}
				} catch (InterruptedException e) {
					log.warning("IndexServer session ended abruptly");
				}
			}).on(IndexServer.queryError, (args1 -> {
				synchronized (fileFailed) {
					fileFailed.add((String) args1[0]);
				}
				log.severe("Could not find " + args1[0] + " on index server " + endpoint.getOtherEndpointId());
				System.out.println("Could not find " + args1[0] + " on index server " + endpoint.getOtherEndpointId());
			}));
			endpoint.emit(IndexServer.queryIndex, query);
		}).on(PeerManager.peerStopped, (args) -> {
			Endpoint endpoint = (Endpoint) args[0];
			log.info("Peer session ended: " + endpoint.getOtherEndpointId());
		}).on(PeerManager.peerError, (args) -> {
			Endpoint endpoint = (Endpoint) args[0];
			log.severe("Peer session ended in error: " + endpoint.getOtherEndpointId());
			endpoint.close();
		});
		clientManager.start();
		clientManager.join(); // wait for the query to finish, since we are in main
		/*
		 * We also have to join with any other client managers that were started for
		 * download purposes.
		 */
		peerManager.joinWithClientManagers();
	}

	private static void help(Options options) {
		String header = "PB Peer\n\n";
		String footer = "\ncontact dsgroup13@unimelb.edu.au for issues.";
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

		assert cmd != null;
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
			synchronized (fileFailed) {
				for (String file: fileFailed) {
					System.out.println("Failed to download: " + file);
				}
			}
		} else {
			System.out.println("must use either the -query or -share option");
			help(options);
		}
		Utils.getInstance().cleanUp();
		log.info("PB Peer stopped");
	}
}
