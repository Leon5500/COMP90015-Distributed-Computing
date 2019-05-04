package unimelb.bitbox;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import java.io.IOException;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import unimelb.bitbox.util.HostPort;
//import unimelb.bitbox.util.FileSystemManager;
//import unimelb.bitbox.util.FileSystemObserver;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;

//import java.security.NoSuchAlgorithmException;
//import java.util.Queue;
//import java.util.LinkedList;
import java.util.ArrayList;
import java.util.HashMap;

//import java.nio.ByteBuffer;
//import java.nio.charset.Charset;



public class ServerMasterThread extends Thread {

    private final String hostS;
    private final int portS;

    private ServerSocket serverSocket;

    private BufferedReader in;
    private BufferedWriter out;

    private final int maxIncomingConnection;
    private ServerMain fileServer;

    private ArrayList<String> conClientList;
    private HashMap<String, RemotePeerInfo> conClientDic;

    private final long maxBlockSize;


    public ServerMasterThread(HostPort server, int maxInCon, ServerMain fileServer, long maxBlockSize) {

        this.hostS = server.host;
        this.portS = server.port;

        serverSocket = null;

        in = null;
        out = null;

        maxIncomingConnection = maxInCon;
        this.fileServer = fileServer;

        //conClientList = new ArrayList<String>();
        //conClientDic = new HashMap<String, ClientInfo>();
        conClientList = fileServer.inConPeerList;
        conClientDic = fileServer.inConPeerDic;

        this.maxBlockSize = maxBlockSize;


        try {
            serverSocket = new ServerSocket(portS);
        }
        catch (SocketException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }




    /**@override*/
    public void run() {

        try {

            int countInCon = 0;

            while(true) {

                // Recycles disconnected server thread and update countInCon
                countInCon = updateClientList(countInCon);

                // Accepts an incoming client connection request 
                Socket socketC = serverSocket.accept();
                countInCon++;

                // input for this request
                in = new BufferedReader(new InputStreamReader(socketC.getInputStream(), "UTF-8"));
                // Output for this request
                out = new BufferedWriter(new OutputStreamWriter(socketC.getOutputStream(), "UTF-8"));

                // Receives an client request
                String hsReq = in.readLine();
                System.out.println("A new incommong client says: " + hsReq);

                // Handles "HANDSHAKE_REQUEST"
                boolean hsSuccess = hsReqHandler(hsReq, countInCon, socketC);

                if(!hsSuccess) {
                    socketC.close();
                    countInCon--;
                }

                in = null;
                out = null;

            }

        }
        catch (SocketException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        finally {
            if(serverSocket!= null) {
                try {
                    serverSocket.close();
                }
                catch (IOException e) {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }



    // Removess disconnected clients and update countInCon

    private int updateClientList(int countInCon){
        ArrayList<String> disConList = new ArrayList<String>();

        try {
            for(int i=0; i<conClientList.size(); i++) {

                String name = conClientList.get(i);

                RemotePeerInfo client = conClientDic.get(name);

                //Pitfall: socket.isClosed() will never return true 
                //         unless the local host closes the socket actively
                //if(client.socket==null || client.socket.isClosed()) {
                if(client.socket==null || !client.socket.getInetAddress().isReachable(3000)) {
                    if(client.receiver!=null) {
                        client.receiver.terminate();
                    }
                    if(client.transmitter!=null) {
                        client.transmitter.terminate();
                    }
                    disConList.add(name);
                    conClientDic.remove(name);
                    System.out.println("Kills client "+name);
                    countInCon--;
                }
                else if(client.receiver==null || client.receiver.getState().equals(Thread.State.valueOf("TERMINATED"))) {
                    if(client.transmitter!=null) {
                        client.transmitter.terminate();
                    }
                    disConList.add(name);
                    conClientDic.remove(name);
                    System.out.println("Kills client "+name);
                    countInCon--;
                }
                else if(client.transmitter==null || client.transmitter.getState().equals(Thread.State.valueOf("TERMINATED"))) {
                    if(client.receiver!=null) {
                        client.receiver.terminate();
                    }
                    disConList.add(name);
                    conClientDic.remove(name);
                    System.out.println("Kills client "+name);
                    countInCon--;
                }
            }
            if(conClientList.size()>0 && disConList.size()>0) {
                conClientList.removeAll(disConList);
            }

            // for verification
            if(countInCon!=conClientList.size()) {
                System.out.println("Warning: bugs in server countInCon");
            }
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Alive clients: "+countInCon);
        return countInCon;
    }


    private boolean hsReqHandler(String hsReq, int countCon, Socket socketC) {

        JSONParser parser = new JSONParser();

        try {
            JSONObject req = (JSONObject) parser.parse(hsReq);

            if(!req.containsKey("command")) {
                sendInvalid("message must contain a command field as string");
                return false;
            }
            else {

                boolean hsStatus;
                String command = (String) req.get("command");

                switch(command)
                {
                    case "HANDSHAKE_REQUEST":
                        if(!req.containsKey("hostPort")) {
                            sendInvalid("HANDSHAKE_REQUEST message must contain a hostPort field");
                            hsStatus = false;
                        }
                        else {
                            JSONObject hostPort = (JSONObject) req.get("hostPort");

                            if(!hostPort.containsKey("host") || !hostPort.containsKey("port")) {
                                sendInvalid("HANDSHAKE_REQUEST message must contain a host or port field");
                                hsStatus = false;
                            }
                            else {
                                if(countCon > maxIncomingConnection) {
                                    sendConRefuse();
                                    hsStatus = false;
                                }
                                else {
                                    System.out.println("From Server: Valid hand shake request!");

                                    String hostC = (String) hostPort.get("host");
                                    int portC = ((Number) hostPort.get("port")).intValue();

                                    TransmitterThread tx = new TransmitterThread(hostC, portC, socketC); 
                                    ReceiverThread rx = new ReceiverThread(hostC, portC, socketC, fileServer, maxBlockSize);

                                    // gen sync event
                                    ArrayList<FileSystemEvent> iniSyncEventList = fileServer.fileSystemManager.generateSyncEvents();
                                    tx.fileEventList.addAll(iniSyncEventList);

                                    String clientName = (new HostPort(hostC, portC)).toString();
                                    RemotePeerInfo clientInfo = new RemotePeerInfo(hostC, portC, socketC, tx, rx);

                                    conClientList.add(clientName);
                                    conClientDic.put(clientName, clientInfo);

                                    sendHSRes();
                                    tx.start();
                                    rx.start();
                                    hsStatus = true;
                                }
                            }
                        }
                        break;
                    default:
                        sendInvalid("invalid HANDSHAKE_REQUEST");
                        hsStatus = false;
                        break;
                }
                return hsStatus;
            }
	}
        catch (ParseException e){
            sendInvalid("server parsing failure");
            return false;
        }
        catch (ClassCastException e){
            sendInvalid("server parsing failure");
            return false;
        }
    }


    private void sendHSRes() {
       try {
            JSONObject hostPortS = new JSONObject();
            hostPortS.put("host", hostS);
            hostPortS.put("port", portS);

            JSONObject hsRes = new JSONObject();
            hsRes.put("command", "HANDSHAKE_RESPONSE");
            hsRes.put("hostPort", hostPortS);

            out.write(hsRes.toJSONString() +"\n");
            out.flush();
            System.out.println(hsRes.toJSONString());
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }


    private void sendConRefuse() {
       try {

            JSONArray peers = new JSONArray();
            for(int i=0; i<conClientList.size(); i++) {
                RemotePeerInfo server = conClientDic.get(conClientList.get(i));

                JSONObject peer = new JSONObject();
                peer.put("host", server.host);
                peer.put("port", server.port);
                peers.add(peer);
            }

            JSONObject conRef = new JSONObject();
            conRef.put("command", "CONNECTION_REFUSED");
            conRef.put("message", "connection limit reached");
            conRef.put("peers", peers);

            out.write(conRef.toJSONString() +"\n");
            out.flush();
            System.out.println(conRef.toJSONString());
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }


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
