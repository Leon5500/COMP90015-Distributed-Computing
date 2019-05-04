package unimelb.bitbox;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

import unimelb.bitbox.util.Configuration;

//New added:
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;
import unimelb.bitbox.util.HostPort;
import java.util.ArrayList;

import java.lang.InterruptedException;



public class Peer {
    private static Logger log = Logger.getLogger(Peer.class.getName());
    public static void main( String[] args ) throws IOException, NumberFormatException, NoSuchAlgorithmException {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                           "[%1$tc] %2$s %4$s: %5$s%n");
        log.info("BitBox Peer starting...");
        Configuration.getConfiguration();

        //new ServerMain();
        ServerMain fileServer = new ServerMain();

        //implemententation begins here:

        //locol peer info
        String hostLocal = Configuration.getConfigurationValue("advertisedName");
        int portLocal = Integer.parseInt(Configuration.getConfigurationValue("port"));
        HostPort peerLocal = new HostPort(hostLocal, portLocal);
        log.info("BitBox Peer Host Port Info "+peerLocal.toString());

        //default outgoing connecting remote peers' info
        String[] peers = Configuration.getConfigurationValue("peers").split(",");
        ArrayList<HostPort> peersRemote = new ArrayList<HostPort>();
        for(String peer: peers) {
            peersRemote.add(new HostPort(peer));
        }

        int maxInCon = Integer.parseInt(Configuration.getConfigurationValue("maximumIncommingConnections"));
        long maxBlockSize = Long.parseLong(Configuration.getConfigurationValue("blockSize"));
 

        ServerMasterThread server = new ServerMasterThread(peerLocal, maxInCon, fileServer, maxBlockSize);
        server.start();

        ClientMasterThread client = new ClientMasterThread(peerLocal, peersRemote, fileServer, maxBlockSize); 
        client.start();


        // Periodic Synchronization
        // General synchronization with all neighboring peers every syncInterval seconds.
        // Appends events generateSyncEvents() to each peer interface's buffer.

        int sInterval = Integer.parseInt(Configuration.getConfigurationValue("syncInterval"));
        int msInterval = 1000 * sInterval;

        while(true) {
            try {
                Thread.sleep(msInterval);
            }
            catch(InterruptedException e) {
                System.out.println(e.getMessage());
            }

           //TBD
           //Dynamic buffer injection issue: 
           //Is it stable enough in complicated/critical operation sequences?

           //to all in con peers
           for(int i=0; i<fileServer.inConPeerList.size(); i++) {
               TransmitterThread tx = fileServer.inConPeerDic.get(fileServer.inConPeerList.get(i)).transmitter;
               if(tx!=null) {
                   ArrayList<FileSystemEvent> syncEventList = fileServer.fileSystemManager.generateSyncEvents();
                   tx.fileEventList.addAll(syncEventList);
               }
           }

           //to all out con peers
           for(int i=0; i<fileServer.outConPeerList.size(); i++) {
               TransmitterThread tx = fileServer.outConPeerDic.get(fileServer.outConPeerList.get(i)).transmitter;

               if(tx!=null) {
                   ArrayList<FileSystemEvent> syncEventList = fileServer.fileSystemManager.generateSyncEvents();
                   tx.fileEventList.addAll(syncEventList);
               }
           }

           log.info("BitBox Peer Periodic Synchronization...");
        }
    }

}
