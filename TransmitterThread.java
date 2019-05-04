package unimelb.bitbox;

import java.net.Socket;

import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.IOException;

import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;
import org.json.simple.JSONObject;

import java.util.ArrayList;

import java.lang.InterruptedException;



// A thread to send file event REQUESTs to a remote peer.
// Events are appended from the Master side and buffered in the fileEventList.

public class TransmitterThread extends Thread {

    private final String hostR;
    private final int portR;

    private Socket socket;
    private BufferedWriter out;

    //this thread's own file event buffer
    protected ArrayList<FileSystemEvent> fileEventList;

    private boolean terminated;



    public TransmitterThread(String hostR, int portR, Socket socket) {

        this.hostR = hostR;
        this.portR = portR;

        this.socket = socket;
        //no input stream for a transmitter
        //output
        try {
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        fileEventList = new ArrayList<FileSystemEvent>();

        terminated = false;

    }



    public void run() {

        System.out.println("Start of this Tx for "+hostR+":"+portR);

        //false if any abnormal status occured e.g. IOException
        boolean status = true;

        //generally keeps monitoring the file event buffer 
        while(!terminated) {

            while(fileEventList.isEmpty()) {
                try {
                    Thread.sleep(50);
                }
                catch(InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }

            if(socket == null) {
                terminate();
            }

            //sync file events in the buffer to the remote peer
            if(!terminated && status) {
                status = fileEventSync();
            }

            //tetminates if abnormal
            if(!status) {
                terminate();
            }
        }

        System.out.println("End of this Tx for "+hostR+":"+portR);

        if(socket != null) {
            try {
                out = null;
                socket.close();
            }
            catch (IOException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }



    //supports high-level control: can be called outside from the Master side
    public synchronized void terminate() {
        terminated = true;
    }


    private boolean fileEventSync() {
        //false if the last synchronization (sending REQUEST) is abnormal
        boolean lastSynSuc = true;

        //processes file events monitered by the FileSystemManager thread
        for(int i=0; i<fileEventList.size(); i++) {
            FileSystemEvent currentEvent = fileEventList.get(i);
            String eventType = currentEvent.event.name();

            switch(eventType) {
                case "FILE_CREATE":
                case "FILE_DELETE":
                case "FILE_MODIFY":
                    lastSynSuc = fileSync(currentEvent);
                    break;
                case "DIRECTORY_CREATE":
                case "DIRECTORY_DELETE":
                    lastSynSuc = dirSync(currentEvent);
                    break;
                default:
                    lastSynSuc = false;
                    break;
            }
            if(!lastSynSuc) {
                break;
            }
        }
        fileEventList.clear();
        return lastSynSuc;
    }



    //extracts parameters of DIRECTORY_CREATE_REQUEST or DIRECTORY_DELETE_REQUEST
    private boolean dirSync(FileSystemEvent fileSystemEvent) {
        String eventType = fileSystemEvent.event.name();
        String pathName = fileSystemEvent.pathName;
        return sendDirReq(eventType, pathName);
    }

   
    //sends DIRECTORY_CREATE_REQUEST or DIRECTORY_DELETE_REQUEST
    private boolean sendDirReq(String eventType, String pathName) {
        try {
            JSONObject dirReq = new JSONObject();
            dirReq.put("command", eventType+"_REQUEST");
            dirReq.put("pathName", pathName);

            out.write(dirReq.toJSONString() +"\n");
            out.flush();
            System.out.println(dirReq.toJSONString());
            return true;
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }



    //extracts parameters of FILE_CREATE_REQUEST, FILE_DELETE_REQUEST, or FILE_MODIFY_REQUEST
    private boolean fileSync(FileSystemEvent fileSystemEvent) {
        String eventType = fileSystemEvent.event.name();
        String pathName = fileSystemEvent.pathName;

        String md5 = fileSystemEvent.fileDescriptor.md5;
        long lastModified = fileSystemEvent.fileDescriptor.lastModified;
        long fileSize = fileSystemEvent.fileDescriptor.fileSize;

        return sendFileReq(eventType, pathName, md5, lastModified, fileSize);
    }


    //sends FILE_CREATE_REQUEST, FILE_DELETE_REQUEST, or FILE_MODIFY_REQUEST
    private boolean sendFileReq(String eventType, String pathName, String md5, long lastModified, long fileSize) {
        try {
            JSONObject fileDescriptor = new JSONObject();
            fileDescriptor.put("md5", md5);
            fileDescriptor.put("lastModified", lastModified);
            fileDescriptor.put("fileSize", fileSize);

            JSONObject fileReq = new JSONObject();
            fileReq.put("command", eventType+"_REQUEST");
            fileReq.put("pathName", pathName);
            fileReq.put("fileDescriptor", fileDescriptor);

            out.write(fileReq.toJSONString() +"\n");
            out.flush();
            System.out.println(fileReq.toJSONString());
            return true;
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}
