package unimelb.bitbox;

import java.net.Socket;

import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.IOException;

import unimelb.bitbox.util.FileSystemManager;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Queue;
import java.util.LinkedList;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.security.NoSuchAlgorithmException;

import java.lang.InterruptedException;



// A thread accepts FILE_BYTES_RESPONSE from its Master ReceiverThread
// then deocdes the bytes content and wtites the target file

public class FileWriterThread extends Thread {

    public final String fRequest;
    public final String fPathName;

    private String fMD5;
    private long fLastModified;
    private long fFileSize;

    public final String hostR;
    public final int portR;

    private Socket socket;
    private BufferedWriter out;

    private FileSystemManager fileManager;

    private final long maxBlockSize;

    //this thread's own FILE_BYTES_RESPONSE message buffer
    protected Queue<String> byteResponseBuffer;



    public FileWriterThread(WriteFileEvent event, Socket socket, FileSystemManager fileManager, long maxBlockSize) {

        fRequest = event.request;
        fPathName = event.pathName;
        fMD5 = event.md5;
        fLastModified = event.lastModified;
        fFileSize = event.fileSize;

        hostR = event.host;
        portR = event.port;

        this.socket = socket;
        //no input stream, input comes from ReceiverThread
        //output
        try {
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        this.fileManager = fileManager;

        this.maxBlockSize = maxBlockSize;

        byteResponseBuffer = new LinkedList<String>();

    }



    public void run() {

        //true if the file is writed successful
        boolean writeSucc = writeFile(fPathName);

        //cancels file loader to prevent blocking further updates
        if(!writeSucc) {
            try {
               fileManager.cancelFileLoader(fPathName);
            }
            catch(IOException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }



    //writes the file until complete or abort
    private synchronized boolean writeFile(String pathName) {
        try {
            int failCount = 0;
            long position = 0;
            long length;
            long remainingSize = fFileSize;

            //abbort if failure count > 3
            while(!fileManager.checkWriteComplete(pathName) && remainingSize>0 && failCount<=3) {
                //prevents from request excessive length
                if(remainingSize < maxBlockSize) {
                    length = remainingSize;
                }
                else {
                    length = maxBlockSize;
                }

                //sends byte request
                boolean req = sendByteReq(pathName, fMD5, fLastModified, fFileSize, position, length);
                //retries if fails up to total 3 times
                while(!req && failCount<=3) {
                    failCount++;
                    req = sendByteReq(pathName, fMD5, fLastModified, fFileSize, position, length);
                }

                //waits for FILE_BYTES_RESPONSE distributed by the ReceiverThread
                while(byteResponseBuffer.isEmpty()) {
                    try {
                        Thread.sleep(50);
                    }
                    catch(InterruptedException e) {
                        System.out.println(e.getMessage());
                    }
                }
                String byteRes = byteResponseBuffer.remove();

                //processes JSON message, writes the file, and checks if any failure
                int result = byteResHandler(byteRes, pathName, position, length);

                //write bytes successfully
                if(result==0){
                    position+=length;
                    remainingSize-=length;
                }

                //updates failure count
                failCount += result;
            }

            return fileManager.checkWriteComplete(pathName);
        }
        catch(IOException | NoSuchAlgorithmException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }



    //sends FILE_BYTES_REQUEST
    private boolean sendByteReq(String pathName, String md5, long lastModified, long fileSize, long position, long length) {
        try {
            JSONObject fileDescriptor = new JSONObject();
            fileDescriptor.put("md5", md5);
            fileDescriptor.put("lastModified", lastModified);
            fileDescriptor.put("fileSize", fileSize);

            JSONObject byteReq = new JSONObject();
            byteReq.put("command", "FILE_BYTES_REQUEST");
            byteReq.put("pathName", pathName);
            byteReq.put("fileDescriptor", fileDescriptor);
            byteReq.put("position", position);
            byteReq.put("length", length);

            out.write(byteReq.toJSONString() +"\n");
            out.flush();
            System.out.println(byteReq.toJSONString());
            return true;
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }



    //processes FILE_BYTES_RESPONSE and writes file
    private int byteResHandler(String byteRes, String reqPathName, long reqPosition, long reqLength) {

        JSONParser parser = new JSONParser();
        try {
            JSONObject res  = (JSONObject) parser.parse(byteRes);

            //checks JSON format, aborts whole writing if invalid message received
            if(!res.containsKey("command")) {
                sendInvalid("message must contain a command field as string");
                return 99;
            }
            else {
                int syncStatus;
                String command = (String) res.get("command");
                String validRes = "FILE_BYTES_RESPONSE";

                if(command.equals(validRes)) {
                    if(!res.containsKey("pathName")) {
                        sendInvalid(validRes+" message must contain a pathName field");
                        syncStatus = 99;
                    }
                    else if(!res.containsKey("message")) {
                        sendInvalid(validRes+" message must contain a message field");
                        syncStatus = 99;
                    }
                    else if(!res.containsKey("status")) {
                        sendInvalid(validRes+" message must contain a status field");
                        syncStatus = 99;
                    }
                    else if(!res.containsKey("fileDescriptor")) {
                        sendInvalid(validRes+" message must contain a fileDescriptor field");
                        syncStatus = 99;
                    }
                    else if(!res.containsKey("position")) {
                        sendInvalid(validRes+" message must contain a position field");
                        syncStatus = 99;
                    }
                    else if(!res.containsKey("length")) {
                        sendInvalid(validRes+" message must contain a length field");
                        syncStatus = 99;
                    }
                    else if(!res.containsKey("content")) {
                        sendInvalid(validRes+" message must contain a content field");
                        syncStatus = 99;
                    }
                    else {
                        String pathName = (String) res.get("pathName");
                        long position = ((Number) res.get("position")).longValue();
                        long length = ((Number) res.get("length")).longValue();
                        String content = (String) res.get("content");
                        boolean status = (boolean) res.get("status");
                        String message = (String) res.get("message");

                        ByteBuffer src;
                        if(content!=null && status) {
                            byte[] decodedContent = Base64.getDecoder().decode(content);
                            src = ByteBuffer.wrap(decodedContent);
                        }
                        else {
                            src = ByteBuffer.allocate(0);
                        }

                        JSONObject fileDescriptor = (JSONObject) res.get("fileDescriptor");

                        if(!pathName.equals(reqPathName)) {
                            sendInvalid("invalid pathName value to our FILE_BYTES_REQUEST");
                            syncStatus = 99;
                        }
                        else if(position != reqPosition) {
                            sendInvalid("invalid position value to our FILE_BYTES_REQUEST");
                            syncStatus = 99;
                        }
                        else if(length != reqLength) {
                            sendInvalid("invalid length value to our FILE_BYTES_REQUEST");
                            syncStatus = 99;
                        }
                        else if(!fileDescriptor.containsKey("md5")) {
                            sendInvalid(validRes+" message must contain a md5 field");
                            syncStatus = 99;
                        }

                        else if(!fileDescriptor.containsKey("lastModified")) {
                            sendInvalid(validRes+" message must contain a lastModified field");
                            syncStatus = 99;
                        }
                        else if(!fileDescriptor.containsKey("fileSize")) {
                            sendInvalid(validRes+" message must contain a fileSize field");
                            syncStatus = 99;
                        }
                        else {
                            String md5 = (String) fileDescriptor.get("md5");
                            long lastModified = ((Number) fileDescriptor.get("lastModified")).longValue();
                            long fileSize = ((Number) fileDescriptor.get("fileSize")).longValue();

                            //the format is correct, writes the file
                            if(status) {
                                boolean writeStatus = fileManager.writeFile(pathName, src, position);
                                syncStatus = (writeStatus)? 0:1;
                            }
                            else {
                                syncStatus = 1;
                            }
                        }
                    }
                }
                else {
                    sendInvalid("invalid response to our FILE_BYTES_REQUEST");
                    syncStatus = 99;
                }
               return syncStatus;
            }
	}
        catch(ParseException e) {
            sendInvalid("JSON parsing failure");
            return 99;
        }
        catch(ClassCastException e){
            sendInvalid("JSON parsing failure");
            return 99;
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return 99;
        }
    }



    //sends INVALID_PROTOCOL
    private void sendInvalid(String msg) {
        try {
            JSONObject invalidPro = new JSONObject();
            invalidPro.put("command", "INVALID_PROTOCOL");
            invalidPro.put("message", msg);

            out.write(invalidPro.toJSONString() +"\n");
            out.flush();
            System.out.println(invalidPro.toJSONString());
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }


}
