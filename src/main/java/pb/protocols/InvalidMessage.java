package pb.protocols;


/**
 * The message is not valid. It may be missing required parameters or the
 * parameters may be of the wrong type. Note that the presence of additional
 * parameters does not trigger this exception.
 * 
 * @author aaron
 *
 */
@SuppressWarnings("serial")
public class InvalidMessage extends Exception {

}
