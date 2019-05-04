package unimelb.bitbox;

import java.net.Socket;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import unimelb.bitbox.util.FileSystemManager;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.HashMap;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.security.NoSuchAlgorithmException;



// A thread accepts REQUESTs and RESOPNSEs from a remote peer then interacts.

public class ReceiverThread extends Thread {

    private final String hostR;
    private final int portR;

    private Socket socket;
    private BufferedReader in;
    private BufferedWriter out;

    private ServerMain fileServer;
    private FileSystemManager fileManager;

    long maxBlockSize;
    //records references of file-writing threads
    private HashMap<String, FileWriterThread> writerDic;

    private boolean terminated;



    ReceiverThread(String hostR, int portR, Socket socket, ServerMain fileServer, long maxBlockSize) {

        this.hostR = hostR;
        this.portR = portR;

        this.socket = socket;
        try {
            //input
            in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
            //output
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        this.fileServer = fileServer;
        fileManager = fileServer.fileSystemManager;

        this.maxBlockSize = maxBlockSize;
        writerDic = new HashMap<String, FileWriterThread>();

        terminated = false;

    }



    public void run() {
        try {
             System.out.println("Start of this Rx for "+hostR+":"+portR);

             while(!terminated) {

                if(socket == null) {
                    terminate();
                }

                //false if the last synchronization is abnormal
                boolean lastSynSuc = true;
                String msg = null;

                while((msg = in.readLine()) != null && lastSynSuc && !terminated) {
                    System.out.println("Remote Host "+hostR+":"+portR+" says: "+msg);
                    lastSynSuc = syncEventHandler(msg);
                }

                if(!lastSynSuc) {
                    terminate();
                }

                if(socket != null) {
                    socket.close();
                }

                if(socket == null || socket.isClosed()) {
                    terminate();
                }
            }

        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        finally {
            if(socket != null) {
                try {
                    in = null;
                    out = null;
                    socket.close();
                    System.out.println("End of this Rx for "+hostR+":"+portR);
                }
                catch (IOException e) {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }



    //supports high-level control: can be called outside from the Master side
    public synchronized void terminate() {
        terminated = true;
    }



    //accepts a JSON message form the remote peer
    private boolean syncEventHandler(String eventIn) {
        JSONParser parser = new JSONParser();

        try {
            JSONObject event = (JSONObject) parser.parse(eventIn);

            //checks if valid protocol format
            if(!event.containsKey("command")) {
                sendInvalid("message must contain a command field as string");
                return false;
            }
            else {
                //false if this synchronization (incoming event) is abnormal
                boolean syncStatus = true;
                String command = (String) event.get("command");

                if(!event.containsKey("pathName")) {
                    sendInvalid("file event message without a pathName field in your "+command);
                    syncStatus = false;
                }
                else {
                    String pathName = (String) event.get("pathName");

                    switch(command) {
                        case "DIRECTORY_CREATE_REQUEST":
                        case "DIRECTORY_DELETE_REQUEST":
                            syncStatus = dirSync(command, pathName);
                            break;
                        case "DIRECTORY_CREATE_RESPONSE":
                        case "DIRECTORY_DELETE_RESPONSE":
                            if(!event.containsKey("message")) {
                                sendInvalid(command+" message must contain a message field");
                                syncStatus = false;
                            }
                            else if(!event.containsKey("status")) {
                                sendInvalid(command+" message must contain a status field");
                                syncStatus = false;
                            }
                            else {
                                 String message = (String) event.get("message");
                                 boolean status = (boolean) event.get("status");
                                 syncStatus = true;
                            }
                            break;
                        case "FILE_CREATE_REQUEST":
                        case "FILE_DELETE_REQUEST":
                        case "FILE_MODIFY_REQUEST":
                        case "FILE_CREATE_RESPONSE":
                        case "FILE_DELETE_RESPONSE":
                        case "FILE_MODIFY_RESPONSE":                          
                        case "FILE_BYTES_REQUEST":

                            if(!event.containsKey("fileDescriptor")) {
                                sendInvalid(command+" message must contain a fileDescriptor field");
                                syncStatus = false;
                            }
                            else {
                                JSONObject fileDescriptor = (JSONObject) event.get("fileDescriptor");
                                if(!fileDescriptor.containsKey("md5")) {
                                    sendInvalid(command+" message must contain a md5 field");
                                    syncStatus = false;
                                }
                                else if(!fileDescriptor.containsKey("lastModified")) {
                                    sendInvalid(command+" message must contain a lastModified field");
                                    syncStatus = false;
                                }
                                else if(!fileDescriptor.containsKey("fileSize")) {
                                    sendInvalid(command+" message must contain a fileSize field");
                                    syncStatus = false;
                                }
                                else {
                                    String md5 = (String) fileDescriptor.get("md5");
                                    long lastModified = ((Number) fileDescriptor.get("lastModified")).longValue();
                                    long fileSize = ((Number) fileDescriptor.get("fileSize")).longValue();

                                    //FILE_REQUEST:
                                    if(command.equals("FILE_CREATE_REQUEST") ||command.equals("FILE_DELETE_REQUEST") || command.equals("FILE_MODIFY_REQUEST")) {
                                        syncStatus = fileSync(command, pathName, md5, lastModified, fileSize);
                                    }
                                    //FILE_RESPONSE:
                                    else if(command.equals("FILE_CREATE_RESPONSE") ||command.equals("FILE_DELETE_RESPONSE") || command.equals("FILE_MODIFY_RESPONSE")) {
                                        if(!event.containsKey("message")) {
                                            sendInvalid(command+" message must contain a message field");
                                            syncStatus = false;
                                        }
                                        else if(!event.containsKey("status")) {
                                            sendInvalid(command+" message must contain a status field");
                                            syncStatus = false;
                                        }
                                        else {
                                            String message = (String) event.get("message");
                                            boolean status = (boolean) event.get("status");
                                            syncStatus = true;
                                        }
                                    }
                                    //FILE_BYTES_REQUEST
                                    else if(command.equals("FILE_BYTES_REQUEST")) {
                                        if(!event.containsKey("position")) {
                                            sendInvalid(command+" message must contain a position field");
                                            syncStatus = false;
                                        }
                                        else if(!event.containsKey("length")) {
                                            sendInvalid(command+" message must contain a length field");
                                            syncStatus = false;
                                        }
                                        else {
                                            long position = ((Number) event.get("position")).longValue();
                                            long length = ((Number) event.get("length")).longValue();
                                            sendByteRes(pathName, md5, lastModified, fileSize, position, length);
                                            syncStatus = true;
                                        }
                                    }
                                }
                            }
                            break;
                        case "FILE_BYTES_RESPONSE":
                            //passes FILE_BYTES_RESPONSE meaasges to corrisponding FileWriterThread's buffer
                            FileWriterThread fileWriter = writerDic.get(pathName);
                            if(fileWriter!=null) {
                                //TBD
                                //Dynamic buffer injection issue: 
                                //Is it stable enough in complicated/critical operation sequences?
                                fileWriter.byteResponseBuffer.add(eventIn);
                            }
                            syncStatus = true;
                        break;
                        default:
                            sendInvalid("invalid file event request or response");
                            syncStatus = false;
                        break;
                    }
                }
                return syncStatus;
            }
        }
        catch(ParseException e) {
            sendInvalid("JSON parsing failure");
            return false;
        }
        catch(ClassCastException e){
            sendInvalid("JSON parsing failure");
            return false;
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



    //handles FILE_CREATE_REQUEST, FILE_DELETE_REQUEST, and FILE_MODIFY_REQUEST
    private boolean fileSync(String fileReq, String pathName, String md5, long lastModified, long fileSize) {
        try{
            if (fileReq.equals("FILE_CREATE_REQUEST")) {
                String validRes = "FILE_CREATE_RESPONSE";

                if(!fileManager.isSafePathName(pathName)) {
                    sendFileRes(validRes, pathName, "unsafe pathname given", false, md5, lastModified, fileSize);
                }
                else if(fileManager.fileNameExists(pathName)) {
                    sendFileRes(validRes, pathName, "pathname already exists", false, md5, lastModified, fileSize);
                }
                else {
                    boolean syncSucc = fileManager.createFileLoader(pathName, md5, fileSize, lastModified);
                    if(syncSucc) {
                        sendFileRes(validRes, pathName, "file loader ready", syncSucc, md5, lastModified, fileSize);

                        if(fileManager.checkShortcut(pathName)) {
                            fileManager.cancelFileLoader(pathName);
                        }
                        else {
                            //registers ans starts a ileWriterThread to write the file
                            WriteFileEvent event = new WriteFileEvent(hostR, portR, fileReq, pathName, md5, lastModified, fileSize);
                            FileWriterThread fileWriter = new FileWriterThread(event, socket, fileManager, maxBlockSize);
                            writerDic.put(pathName, fileWriter);
                            fileWriter.start();
                        }
                    }
                    else {
                        //tries to resolve the createFileLoader fail by deleting an obsolete file loader
                        fileManager.cancelFileLoader(pathName);
                        sendFileRes(validRes, pathName, "there was a problem creating the file", syncSucc, md5, lastModified, fileSize);
                    }
                }
                return true;
            }

            else if (fileReq.equals("FILE_DELETE_REQUEST")) {
                String validRes = "FILE_DELETE_RESPONSE";

                if(!fileManager.isSafePathName(pathName)) {
                    sendFileRes(validRes, pathName, "unsafe pathname given", false, md5, lastModified, fileSize);
                }
                else if(!fileManager.fileNameExists(pathName)) {
                    sendFileRes(validRes, pathName, "pathname does not exist", false, md5, lastModified, fileSize);
                }
                else {
                    boolean syncSucc =  fileManager.deleteFile(pathName, lastModified, md5);
                    if(syncSucc) {
                        sendFileRes(validRes, pathName, "file deleted", syncSucc, md5, lastModified, fileSize);
                    }
                    else {
                        //tries to resolve the deleteFile fail by deleting an obsolete file loader
                        fileManager.cancelFileLoader(pathName);
                        sendFileRes(validRes, pathName, "there was a problem deleting the file", syncSucc, md5, lastModified, fileSize);
                    }
                }
                return true;
            }

            else if (fileReq.equals("FILE_MODIFY_REQUEST")) {
                String validRes = "FILE_MODIFY_RESPONSE";

                if(!fileManager.isSafePathName(pathName)) {
                    sendFileRes(validRes, pathName, "unsafe pathname given", false, md5, lastModified, fileSize);
                }
                else if(!fileManager.fileNameExists(pathName)) {
                    sendFileRes(validRes, pathName, "pathname does not exist", false, md5, lastModified, fileSize);
                }
                else if(fileManager.fileNameExists(pathName, md5)) {
                    sendFileRes(validRes, pathName, "file already exists with matching content", false, md5, lastModified, fileSize);
                }
                else {
                    boolean syncSucc = fileManager.modifyFileLoader(pathName, md5, lastModified);
                    if(syncSucc) {
                        sendFileRes(validRes, pathName, "file loader ready", syncSucc, md5, lastModified, fileSize);

                        if(fileManager.checkShortcut(pathName)) {
                            fileManager.cancelFileLoader(pathName);
                        }
                        else {
                            //registers ans starts a ileWriterThread to write the file                       
                            WriteFileEvent event = new WriteFileEvent(hostR, portR, fileReq, pathName, md5, lastModified, fileSize);
                            FileWriterThread fileWriter = new FileWriterThread(event, socket, fileManager, maxBlockSize);
                            writerDic.put(pathName, fileWriter);
                            fileWriter.start();
                        }
                    }
                    else {
                        //tries to resolve the deleteFile fail by deleting an obsolete file loader
                        fileManager.cancelFileLoader(pathName);
                        sendFileRes(validRes, pathName, "there was a problem modifying the file", syncSucc, md5, lastModified, fileSize);
                    }
                 }
                 return true;
            }

            else {
                sendInvalid("invalid file event request");
                return false;
            }
        }
        catch(IOException | NoSuchAlgorithmException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }


    //sends FILE_CREATE_RESPONSE, FILE_DELETE_RESPONSE, and FILE_MODIFY_RESPONSE
    private void sendFileRes(String eventRes, String pathName, String msg, boolean status, String md5, long lastModified, long fileSize) {
        try {

            JSONObject fileDescriptor = new JSONObject();
            fileDescriptor.put("md5", md5);
            fileDescriptor.put("lastModified", lastModified);
            fileDescriptor.put("fileSize", fileSize);

            JSONObject fileRes = new JSONObject();
            fileRes.put("command", eventRes);
            fileRes.put("pathName", pathName);
            fileRes.put("fileDescriptor", fileDescriptor);
            fileRes.put("message", msg);
            fileRes.put("status", status);

            out.write(fileRes.toJSONString() +"\n");
            out.flush();
            System.out.println(fileRes.toJSONString());
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }



    //handles DIRECTORY_CREATE_REQUEST and DIRECTORY_DELETE_REQUEST
    private boolean dirSync(String dirReq, String pathName) {

        if (dirReq.equals("DIRECTORY_CREATE_REQUEST")) {
            String validRes = "DIRECTORY_CREATE_RESPONSE";

            if(!fileManager.isSafePathName(pathName)) {
                sendDirRes(validRes, pathName, "unsafe pathname given", false);
            }
            else if(fileManager.dirNameExists(pathName)) {
                sendDirRes(validRes, pathName, "pathname already exists", false);
            }
            else {
                boolean syncSucc = fileManager.makeDirectory(pathName);
                if(syncSucc) {
                    sendDirRes(validRes, pathName, "directory created", syncSucc);
                }
                else {
                    sendDirRes(validRes, pathName, "there was a problem creating the directory", syncSucc);
                }
            }
            return true;
        }

        else if (dirReq.equals("DIRECTORY_DELETE_REQUEST")) {
            String validRes = "DIRECTORY_DELETE_RESPONSE";

            if(!fileManager.isSafePathName(pathName)) {
                sendDirRes(validRes, pathName, "unsafe pathname given", false);
            }
            else if(!fileManager.dirNameExists(pathName)) {
                sendDirRes(validRes, pathName, "pathname does not exist", false);
            }
            else {
                boolean syncSucc =  fileManager.deleteDirectory(pathName);
                if(syncSucc) {
                    sendDirRes(validRes, pathName, "directory deleted", syncSucc);
                }
                else {
                    sendDirRes(validRes, pathName, "there was a problem deleting the directory", syncSucc);
                }
            }
            return true;
        }

        else {
            sendInvalid("invalid directory event request");
            return false;
        }

    }


    //sends DIRECTORY_CREATE_RESPONSE and DIRECTORY_DELETE_RESPONSE
    private void sendDirRes(String eventRes, String pathName, String msg, boolean status) {
        try {
            JSONObject dirRes = new JSONObject();
            dirRes.put("command", eventRes);
            dirRes.put("pathName", pathName);
            dirRes.put("message", msg);
            dirRes.put("status", status);

            out.write(dirRes.toJSONString() +"\n");
            out.flush();
            System.out.println(dirRes.toJSONString());
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }



    //reads the file bytes and sends FILE_BYTES_RESPONSE
    private void sendByteRes(String pathName, String md5, long lastModified, long fileSize, long position, long length) {
        try {

            JSONObject fileDescriptor = new JSONObject();
            fileDescriptor.put("md5", md5);
            fileDescriptor.put("lastModified", lastModified);
            fileDescriptor.put("fileSize", fileSize);

            JSONObject byteRes = new JSONObject();
            byteRes.put("command", "FILE_BYTES_RESPONSE");
            byteRes.put("pathName", pathName);
            byteRes.put("fileDescriptor", fileDescriptor);
            byteRes.put("position", position);
            byteRes.put("length", length);

            //protects for length exceeding the remaining file size
            long readLength = length;
            if(readLength>fileSize-position) {
                readLength=fileSize-position;
            }


            //ByteBuffer content = fileManager.readFile(md5, position, length);
            ByteBuffer content = fileManager.readFile(md5, position, readLength);

            String message;
            boolean status;

            String encodedContent;
            if(content==null) {
                message = "unsuccessful read";
                status = false;
                encodedContent = null;
            }
            else {
                message =  "successful read";
                status = true;
                encodedContent = new String(Base64.getEncoder().encode(content.array()));
            }
            //byteRes.put("content", content);
            byteRes.put("content", encodedContent);
            byteRes.put("message", message);
            byteRes.put("status", status);

            out.write(byteRes.toJSONString() +"\n");
            out.flush();
            System.out.println(byteRes.toJSONString());
        }
        catch(IOException | NoSuchAlgorithmException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }


}
