package unimelb.bitbox;

import java.net.Socket;

// Records an instance of our peer infterface connecting with a remote peer.
// Our peer interface consists of:
// A socket to a remote peer.
// A transmitter thread to accept all input stream and send RESPONSEs to REQUESTs
// A receiver thread sends all updated events' REQUEST.
// (and maybe several file writer threads to write files parallelly)

public class RemotePeerInfo {

    //remote peer info
    String host;
    int port;

    Socket socket;
    TransmitterThread transmitter;
    ReceiverThread receiver;

    public RemotePeerInfo(String host, int port, Socket socket, 
                          TransmitterThread transmitter, 
                          ReceiverThread receiver) {
        this.host = host;
        this.port = port;
        this.socket = socket;
        this.transmitter = transmitter;
        this.receiver = receiver;
    }
}
