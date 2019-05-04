package unimelb.bitbox;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.FileSystemManager;
import unimelb.bitbox.util.FileSystemObserver;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;

//New added:
import java.util.ArrayList;
import java.util.HashMap;

// We view this object as a global area to record alive threads' info,
// store monitered file events, and process files.
// Pass the reference of ServerMain to allow access.

public class ServerMain implements FileSystemObserver {
    private static Logger log = Logger.getLogger(ServerMain.class.getName());
    protected FileSystemManager fileSystemManager;

    //New added:
    //buffer to contain all FileSystemEvents
    protected ArrayList<FileSystemEvent> fileSystemEventList;

    //Records all incoming connecting peers' info
    protected ArrayList<String> inConPeerList;
    protected HashMap<String, RemotePeerInfo> inConPeerDic;

    //Records all outgoing connecting peers' info
    protected ArrayList<String> outConPeerList;
    protected HashMap<String, RemotePeerInfo> outConPeerDic;


    public ServerMain() throws NumberFormatException, IOException, NoSuchAlgorithmException {
        //file processing interface
        fileSystemManager=new FileSystemManager(Configuration.getConfigurationValue("path"),this);
        
        //implemententation begins here:
        fileSystemEventList = new ArrayList<FileSystemEvent>();

        inConPeerList = new ArrayList<String>();
        inConPeerDic = new HashMap<String, RemotePeerInfo>();

        outConPeerList = new ArrayList<String>();
        outConPeerDic = new HashMap<String, RemotePeerInfo>();
    }

    //Simply buffers all monitored events.
    @Override
    public void processFileSystemEvent(FileSystemEvent fileSystemEvent) {
        //System.out.println("From ServerMain: "+fileSystemEvent.toString());
        synchronized(this) {
            fileSystemEventList.add(fileSystemEvent);
        }
    }

}
