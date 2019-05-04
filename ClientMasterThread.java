package unimelb.bitbox;

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

import java.util.ArrayList;
import java.util.LinkedList;
//import java.util.ListIterator;
import java.util.Queue;
import java.util.HashMap;
//import java.nio.ByteBuffer;
//import java.security.NoSuchAlgorithmException;

import java.lang.InterruptedException;

//import java.lang.Thread.State;
import java.util.Iterator;


public class ClientMasterThread extends Thread {



    private final String hostC;
    private final int portC;

    private ArrayList<HostPort> servers;

    private BufferedReader in;
    private BufferedWriter out;

    private ServerMain fileServer;
    private final long maxBlockSize;


    private ArrayList<String> conServerList;
    private HashMap<String, RemotePeerInfo> conServerDic;

    private ArrayList<String> conClientList;
    private HashMap<String, RemotePeerInfo> conClientDic;

    //for BFS
    private Queue<String> discoveredPeerList;
    private HashMap<String, String> discoveredPeerDic;
                


    public ClientMasterThread(HostPort client, ArrayList<HostPort> servers, ServerMain fileServer, long maxBlockSize) {

        this.hostC = client.host;
        this.portC = client.port;

        this.servers = servers;

        in = null;
        out = null;
  
        this.fileServer = fileServer;
        this.maxBlockSize = maxBlockSize;

        //conServerList = new ArrayList<String>();
        //conServerDic = new HashMap<String, ServerInfo>();
        conServerList = fileServer.outConPeerList;
        conServerDic = fileServer.outConPeerDic;

        conClientList = fileServer.inConPeerList;
        conClientDic = fileServer.inConPeerDic;

        discoveredPeerList = new LinkedList<String>();
        discoveredPeerDic = new HashMap<String, String>();

    }


    public void run() {

        while(true) {

            updateServerList();

            try {
                Thread.sleep(500);
            }
            catch(InterruptedException e) {
                System.out.println(e.getMessage());
            }


            if(!fileServer.fileSystemEventList.isEmpty()) {

                //initialize slave clients
                for(int i=0; i<servers.size(); i++) {
                    String hostS = servers.get(i).host;
                    int portS = servers.get(i).port;
                    String serverName = (new HostPort(hostS, portS)).toString();

                    if(!conServerDic.containsKey(serverName)) {
                        startConnection(hostS, portS, true);
                    }
                }

                //update new events
                for(int i=0; i<conServerList.size(); i++) {
                    TransmitterThread tx = conServerDic.get(conServerList.get(i)).transmitter;
                    if(tx!=null) {
                        if(tx.getState().equals(Thread.State.valueOf("NEW"))) {
                            ArrayList<FileSystemEvent> iniSyncEventList = fileServer.fileSystemManager.generateSyncEvents();
                            tx.fileEventList.addAll(iniSyncEventList);
                        }

                        for(int j=0; j<fileServer.fileSystemEventList.size(); j++) {
                            //TBD
                            //Dynamic buffer injection issue: 
                            //Is it stable enough in complicated/critical operation sequences?

                            FileSystemEvent task = fileServer.fileSystemEventList.get(j);
                            tx.fileEventList.add(task);
                        }
                    }
                }
                for(int i=0; i<conClientList.size(); i++) {
                    TransmitterThread tx = conClientDic.get(conClientList.get(i)).transmitter;
                    if(tx!=null) {
                        for(int j=0; j<fileServer.fileSystemEventList.size(); j++) {
                            //TBD
                            //Dynamic buffer injection issue: 
                            //Is it stable enough in complicated/critical operation sequences?

                            FileSystemEvent task = fileServer.fileSystemEventList.get(j);
                            tx.fileEventList.add(task);
                        }
                    }
                }
                fileServer.fileSystemEventList.clear();
            }


            for(int i=0; i<conServerList.size(); i++) {
                TransmitterThread tx = conServerDic.get(conServerList.get(i)).transmitter;
                ReceiverThread rx = conServerDic.get(conServerList.get(i)).receiver;

                if(tx!=null && rx!=null) {
                    if(tx.getState().equals(Thread.State.valueOf("NEW"))) {
                        tx.start();
                        rx.start();
                   }
                }
            }
                
        }
    }



    // Recycles disconnected client thread
    private void updateServerList(){
        ArrayList<String> disConList = new ArrayList<String>();

        try {

            for(int i=0; i<conServerList.size(); i++) {

                String name = conServerList.get(i);
                RemotePeerInfo server = conServerDic.get(name);

                //Pitfall: socket.isClosed() will never return true 
                //         unless the local host closes the socket actively
                //if(server.socket==null || server.socket.isClosed()) {
                if(server.socket==null || !server.socket.getInetAddress().isReachable(3000)) {
                    if(server.receiver!=null) {
                        server.receiver.terminate();
                    }
                    if(server.transmitter!=null) {
                        server.transmitter.terminate();
                    }
                    disConList.add(name);
                    conServerDic.remove(name);
                    System.out.println("Kills server "+name);
                }
                else if(server.receiver==null || server.receiver.getState().equals(Thread.State.valueOf("TERMINATED"))) {
                    if(server.transmitter!=null) {
                        server.transmitter.terminate();
                    }
                    disConList.add(name);
                    conServerDic.remove(name);
                    System.out.println("Kills server "+name);
                }
                else if(server.transmitter==null || server.transmitter.getState().equals(Thread.State.valueOf("TERMINATED"))) {
                    if(server.receiver!=null) {
                        server.receiver.terminate();
                    }
                    disConList.add(name);
                    conServerDic.remove(name);
                    System.out.println("Kills server "+name);
                }

            }
            if(conServerList.size()>0 && disConList.size()>0) {
                conServerList.removeAll(disConList);
            }
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }



    private boolean startConnection(String hostS, int portS, boolean doBFS) {
        try {

            Socket socketS = new Socket(hostS, portS);
            boolean hsSuccess = false;

            if(socketS!=null) {
                //input
                in = new BufferedReader(new InputStreamReader(socketS.getInputStream(), "UTF-8"));
                //output
                out = new BufferedWriter(new OutputStreamWriter(socketS.getOutputStream(), "UTF-8"));

                // Issues a Hand Shake Request
                sendHSReq();

                // Receives a server response
                String hsRes = in.readLine();
                System.out.println("Server Says: "+hsRes);

                hsSuccess = hsResHandler(hsRes, hostS, portS, socketS, doBFS);

                in = null;
                out = null;
            }

            return hsSuccess;
        }
        catch (SocketException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
            return false;
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }


    private boolean hsResHandler(String hsRes, String hostS, int portS, Socket socketS, boolean doBFS) {

        JSONParser parser = new JSONParser();

        try {
            JSONObject res = (JSONObject) parser.parse(hsRes);

            if(!res.containsKey("command")) {
                sendInvalid("message must contain a command field as string");
                return false;
            }
            else {

                boolean hsStatus;
                String command = (String) res.get("command");

                switch(command)
                {
                    case "HANDSHAKE_RESPONSE":
                        if(!res.containsKey("hostPort")) {
                            sendInvalid("HANDSHAKE_RESPONSE message must contain a hostPort field");
                            hsStatus = false;
                        }
                        else {
                            JSONObject hostPort = (JSONObject) res.get("hostPort");

                            if(!hostPort.containsKey("host") || !hostPort.containsKey("port")) {
                                sendInvalid("HANDSHAKE_RESPONSE message must contain a host and a port fields");
                                hsStatus = false;
                            }
                            else {
                                String host = (String) hostPort.get("host");
                                int port = ((Number) hostPort.get("port")).intValue();

                                if(!host.equals(hostS) || port!=portS) {
                                    sendInvalid("ambiguous HANDSHAKE_RESPONSE host or port info");
                                    hsStatus = false;
                                }
                                else {
                                    System.out.println("From Client: Successful hand shake!");

                                    TransmitterThread tx = new TransmitterThread(hostS, portS, socketS); 
                                    ReceiverThread rx = new ReceiverThread(hostS, portS, socketS, fileServer, maxBlockSize);

                                    String serverName = (new HostPort(hostS, portS)).toString();
                                    RemotePeerInfo serverInfo = new RemotePeerInfo(hostS, portS, socketS, tx, rx);

                                    conServerList.add(serverName);
                                    conServerDic.put(serverName, serverInfo);

                                    hsStatus = true;
                                }
                            }
                        }
                        break;

                    case "CONNECTION_REFUSED":
                        if(doBFS) {

                            if(!res.containsKey("peers")) {
                                sendInvalid("CONNECTION_REFUSED message must contain a hostPort field");
                                hsStatus = false;
                            }

                            else if(!res.containsKey("message")) {
                                sendInvalid("CONNECTION_REFUSED message must contain a message field");
                                hsStatus = false;
                            }
                            else {
                                JSONArray peers = (JSONArray) res.get("peers");
                                Iterator<JSONObject> it = peers.iterator();
                                ArrayList<String> peerList = new ArrayList<String>();
                                while(it.hasNext()) {

                                    JSONObject hostPort = it.next();
                                    if(!hostPort.containsKey("host") || !hostPort.containsKey("port")) {
                                        sendInvalid("CONNECTION_REFUSED message must contain hosts and ports in peers field");
                                        hsStatus = false;
                                        break;
                                     }
                                     else {
                                         String host = (String) hostPort.get("host");
                                         int port = ((Number) hostPort.get("port")).intValue();
                                         String peer = (new HostPort(host, port)).toString();
                                         peerList.add(peer);
                                     }
                                }
                                hsStatus = startConnectionBFS(peerList);
                            }
                        }
                        //Ends without a BFS. TBD: backups for futher applications.
                        else {
                            hsStatus = false;
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
            sendInvalid("client parsing failure");
            return false;
        }
        catch (ClassCastException e){
            sendInvalid("client parsing failure");
            return false;
        }
    }


    private void sendHSReq() {
        try {
            JSONObject hostPortC = new JSONObject();
            hostPortC.put("host", hostC);
            hostPortC.put("port", portC);

            JSONObject hsReq = new JSONObject();
            hsReq.put("command", "HANDSHAKE_REQUEST");
            hsReq.put("hostPort", hostPortC);

            out.write(hsReq.toJSONString() +"\n");
            out.flush();
            System.out.println(hsReq.toJSONString());
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


    private boolean startConnectionBFS(ArrayList<String> peerList) {

        boolean success = false;

        for(int i=0; i< peerList.size(); i++) {
            String peer = peerList.get(i);

            if(!discoveredPeerDic.containsKey(peer)) {
                discoveredPeerDic.put(peer, peer);
                discoveredPeerList.add(peer);
            }
        }

        while(!discoveredPeerList.isEmpty() && !success) {
            String visitedPeer = discoveredPeerList.remove();

            if(!conServerDic.containsKey(visitedPeer) && !conClientDic.containsKey(visitedPeer)) {
               HostPort peer = new HostPort(visitedPeer);
               String host = peer.host;
               int port = peer.port;
               success = startConnection(host, port, true);
            }
        }

        discoveredPeerDic.clear();
        discoveredPeerList.clear();
        return success;

    }
}
