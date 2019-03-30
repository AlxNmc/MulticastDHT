import java.io.*;
import java.net.*;
import java.util.*;

//Maintains record of the DHT group. Only the root node has access to this
class DHTrecord{
    private static ArrayList<Integer> list;

    DHTrecord(){
        list = new ArrayList<>();
    }
    //returns neighbors of node with given ID
    public static Integer[] get(int ID){
        int index = list.indexOf(ID);
        //if not in DHT, return null
        if(index<0) return  null;
        //otherwise return nodes on either side of that ID
        Integer[] neighbors = new Integer[2];
        int size = list.size();
        neighbors[0] = index==0 ? list.get(size-1):list.get(index-1);
        neighbors[1] = index==size-1 ? list.get(0):list.get(index+1);
        return neighbors;
    }
    //adds ID to record and returns neighbors
    public static Integer[] set(int ID){
        if(list.contains(ID)) return null;
        list.add(ID);
        Collections.sort(list);
        return get(ID);
    }
}

//keeps record of all multicast groups and this node's membership status
class McastRecord{
    private static HashMap<Integer, Boolean> map;
    McastRecord(){
        map = new HashMap<>();
    }
    //check if this node is a part of a multicast group
    public static boolean checkMembership(int McastID){
        if(map.containsKey(McastID)){
            return map.get(McastID);
        }else return false;
    }
    //add new multicast group or change membership
    public static void add(int McastID, boolean member){
        if(map.containsKey(McastID)){
            map.replace(McastID, member);
        }else map.put(McastID, member);
    }
}

//Handles messages regarding multicasting
class McastWorker extends Thread{
    String msg;
    McastWorker(String message){
        msg = message;
    }
    public void run(){
        //handle each case with appropriate method
        if(msg.indexOf("create")>=0){create();}
        else if(msg.indexOf("add")>=0){add();}
        else if(msg.indexOf("send")>=0){send();}
    }

    //checks if message is intended for group that this node is a member of
    //if so, prints message.
    void send(){
        String[] splitMsg = msg.trim().split("\\s+");
        int mID = Integer.parseInt(splitMsg[1]);
        if(McastRecord.checkMembership(mID)){
            String message = msg.replaceAll(splitMsg[0], "");
            message = message.replaceAll(splitMsg[1], "").trim();
            System.out.println("Message for group " + mID + ": " + message + "\n");
        }
        //only send to successor if the message did not originate at this node
        if(msg.indexOf(" " + McastDHT.ID + "mcast")>=0){
            McastDHT.send(msg, NodeListener.successor+40000);
        }
    }

    //add this node to the given multicast group
    void add(){
        int mID = Integer.parseInt(msg.split("\\s+")[1]);
        System.out.println("Joining group " + mID + "\n");
        //alter entry for this ID in MulticastRecord accordingly
        McastRecord.add(mID, true);
    }

    //create a new multicast group
    void create(){
        int mID = Integer.parseInt(msg.trim().split("\\s+")[1]);
        System.out.println("Adding group " + mID + "\n");
        McastRecord.add(mID, false);
        //send message to successor if it did not originate from this node
        if(msg.indexOf( " " + Integer.toString(McastDHT.ID)+"mcast")<0){
            McastDHT.send(msg, NodeListener.successor+40000);
        }
    }
}

class RootWorker extends Thread{
    DatagramSocket socket;
    DatagramPacket packet;
    RootWorker(DatagramPacket packet, DatagramSocket socket){
        this.packet = packet;
        this.socket = socket;
    }
    public void run(){
        System.out.println("Starting Root Worker");
        byte[] buffer;
        String msg;
        InetAddress address;
        int port;
        try{
            address = packet.getAddress();
            port = packet.getPort();
            msg = new String(packet.getData(), 0, packet.getLength());
            if(msg.indexOf("test")>=0){
                System.out.println("New client connected.");
                buffer = msg.getBytes();
                packet = new DatagramPacket(buffer, buffer.length, address, port);
                socket.send(packet);
            }else if(msg.indexOf("Assuming")>=0){
                System.out.println("Processing new client ID.");
                int NodeID = Integer.parseInt(msg.split("\\s+")[1]);
                Integer[] neighbors = DHTrecord.get(NodeID);
                if(neighbors==null){
                    neighbors = DHTrecord.set(NodeID);
                    updateNeighbors(neighbors); //send new predecessors/successors to neighbors
                    msg = "Neighbors " + neighbors[0] + " " + neighbors[1];
                    System.out.println("New ID: " + NodeID);
                }else{
                    msg = "Taken";
                }
                buffer = msg.getBytes();
                packet = new DatagramPacket(buffer, buffer.length, address, port);
                System.out.println("Sending " + msg);
                socket.send(packet);
            }
        }catch (Exception ex){ex.printStackTrace();}
    }
    //send message containing updated predecessors and successors to the two nodes
    void updateNeighbors(Integer[] neighbors){
        DatagramSocket socket;
        DatagramPacket packet0;
        DatagramPacket packet1;
        InetAddress address;
        Integer[] neighbors0 = DHTrecord.get(neighbors[0]);
        Integer[] neighbors1 = DHTrecord.get(neighbors[1]);
        byte[] buffer;
        String msg;
        try{
            address = InetAddress.getByName("localhost");
            socket = new DatagramSocket();
            msg = "Neighbors " + neighbors0[0] + " " + neighbors0[1];
            buffer = msg.getBytes();
            packet0 = new DatagramPacket(buffer, buffer.length, address, 40000+neighbors[0]);
            msg = "Neighbors " + neighbors1[0] + " " + neighbors1[1];
            buffer = msg.getBytes();
            packet1 = new DatagramPacket(buffer, buffer.length, address, 40000+neighbors[1]);
            socket.send(packet0);
            socket.send(packet1);
        }catch(Exception ex){ex.printStackTrace();}
    }
}

class NodeListener extends Thread{
    static int predecessor;
    static int successor;

    NodeListener(int p, int s){
        predecessor = p;
        successor = s;
    }

    public void run(){
        displayInfo();
        new InputManager().start(); //start gathering input from client
        DatagramPacket packet;
        DatagramSocket socket;
        byte[] buffer;
        try{
            socket = new DatagramSocket(40000+McastDHT.ID);
            while(true){
                buffer = new byte[256];
                packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String msg = new String(packet.getData(), 0, packet.getLength());

                if(msg.indexOf("Neighbors")>=0){updateNeighbors(msg);}
                else if(msg.indexOf("ping")>=0){ping(msg);}
                else if(msg.indexOf("survey")>=0){survey(msg);}
                else if(msg.indexOf("mcast")>=0){new McastWorker(msg).start();}
                else if(McastDHT.ID==1){new RootWorker(packet, socket).start();}
            }
        }catch(Exception ex){ex.printStackTrace();}
    }
    //updates predecessor and successor for this node
    void updateNeighbors(String msg){
        String[] splitMsg = msg.split("\\s+");
        predecessor = Integer.parseInt(splitMsg[1]);
        successor = Integer.parseInt(splitMsg[2]);
        System.out.println("Status updated.");
        displayInfo();
    }

    public static void displayInfo(){
        System.out.printf("ID: %d predecessor: %d successor: %d\n\n", McastDHT.ID, predecessor, successor);
    }

    void ping(String msg){
        String[] splitMsg = msg.split("\\s+");
        String response;
        int port;
        boolean respond = true;
        if(msg.indexOf("loop")>=0){
            System.out.println("Loop Ping: " + splitMsg[splitMsg.length-1] + "\n");
            response = msg;
            port = NodeListener.successor + 40000;
            if(Integer.parseInt(splitMsg[0])==McastDHT.ID){
                return;
            }
        }else{
            port = Integer.parseInt(msg.split("\\s+")[1])+40000;
            System.out.println("Ping from " + (port-40000) + "\n");
            if(msg.indexOf("response")>=0){
               return;
            }
            response = "pingresponse " + McastDHT.ID;
        }
        DatagramPacket packet;
        if(respond) {
            DatagramSocket socket;
            InetAddress address;
            byte[] buffer;
            try {
                socket = new DatagramSocket();
                address = InetAddress.getByName("localhost");
                buffer = response.getBytes();
                packet = new DatagramPacket(buffer, buffer.length, address, port);
                socket.send(packet);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

    }

    void survey(String msg){
        if(msg.contains(" " + Integer.toString(McastDHT.ID) + " ")){
            System.out.println("Nodes in DHT: ");
            msg = msg.replaceAll("survey ", "");
            for(String s: msg.split("\\s+")){
                System.out.println(s);
            }
            System.out.println();
        }else{
            msg = msg + " " + McastDHT.ID + " ";
            McastDHT.send(msg, NodeListener.successor + 40000);
        }
    }

}

class NodeClient extends Thread{
    String request;
    NodeClient(String input){
        request = input;
    }
    public void run(){
        if(request.indexOf("status")>=0){
            NodeListener.displayInfo();
        }else if(request.indexOf("ping")>=0){
            sendPing(request);
        }else if(request.indexOf("survey")>=0){
            survey();
        }else if(request.indexOf("mcast")>=0){
            mcast(request);
        }
    }

    //handle the various mcast-related requests
    void mcast(String request){
        String[] splitMsg = request.trim().split("\\s++");
        String mID = splitMsg[2];
        if(request.indexOf("create")>=0){
            String msg = " " + McastDHT.ID + "mcastcreate " + mID;
            McastDHT.send(msg, NodeListener.successor+40000);
        }else if(request.indexOf("add")>=0){
            int port = Integer.parseInt(splitMsg[3])+40000;
            String msg = "mcastadd " + mID;
            McastDHT.send(msg, port);
        }else if(request.indexOf("send")>=0){
            String msg = " " + McastDHT.ID + "mcastsend" + request.replaceAll("mcast send", "");
            McastDHT.send(msg, NodeListener.successor+40000);
        }
    }

    //send survey request to DHT members
    void survey(){
        String msg;
        int port;
        //add this node's id to the record
        msg = "survey " + McastDHT.ID + " ";
        port = NodeListener.successor + 40000;
        McastDHT.send(msg, port);
    }

    //send pind message to intended node
    void sendPing(String request){
        String msg;
        int port;

        if(request.indexOf("loop")>=0){
            msg = McastDHT.ID + " " + request;
            port = NodeListener.successor + 40000;
        }else{
            msg = "ping " + McastDHT.ID;
            port = Integer.parseInt(request.split("\\s+")[1]);
        }
        McastDHT.send(msg, port);
    }
}

//handles input from user
class InputManager extends Thread{
    public void run(){
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        try{
            printCommands();
            String input;

            //read and process line of input
            while(true){
                input = in.readLine();
                input = input.toLowerCase();
                new NodeClient(input).start();
            }

        }catch (IOException ex){ex.printStackTrace();}
    }
    void printCommands(){
        System.out.println("Available commands:");
        System.out.println("\"Status\" - Display Node ID, Predecessor, and Successor.");
        System.out.println("\"Ping [ComPort]\" - Ping node at this ComPort (Node ID + 40000).");
        System.out.println("\"LoopPing\" - Send ping around the DHT.");
        System.out.println("\"Survey\" - Generate list of all Node IDs in the DHT.");
        System.out.println("\"Mcast Create [McastID]\" - Create a new multicast group.");
        System.out.println("\"Mcast Add [McastID] [NodeID]\" - Add a node to multicast group.");
        System.out.println("\"Mcast Send [McastID] message\" - Send message to multicast group.\n");
    }
}

public class McastDHT {
    public static  int ID;

    public static void send(String msg, int port){
        DatagramSocket socket;
        InetAddress address;
        byte[] buffer;

        try{
            socket = new DatagramSocket();
            address = InetAddress.getByName("localhost");
            buffer = msg.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, port);
            socket.send(packet);
        }catch(Exception ex){ex.printStackTrace();}
    }

    public static void main(String[] args) {
        boolean amRoot = false;
        //Try to connect to port 40001. If cannot, then assume original-node duties and DHT-ID of 1
        DatagramSocket socket;
        DatagramPacket packet;
        InetAddress address;
        byte[] buffer;
        int timeout = 2; //number of seconds to wait for root node response before assuming root node role

        try {
            //set socket, address, and UDP message
            socket = new DatagramSocket();
            address = InetAddress.getByName("localhost");
            buffer = "test".getBytes();

            //send message to port 40001
            packet = new DatagramPacket(buffer, buffer.length, address, 40001);
            socket.send(packet);

            //listen for response
            while(true){
            packet = new DatagramPacket(buffer, buffer.length);
            socket.setSoTimeout(timeout*1000);
            try{
                socket.receive(packet);
            }catch(SocketTimeoutException ex){
                System.out.println("Assuming role of root node.");
                ID = 1;
                amRoot = true;
                break;
            }
            socket.close();
            break;
            }
        }catch(Exception ex){ ex.printStackTrace(); }

        //handle root Node case
        if(amRoot){
            //create DHTrecord object to hold node IDs
            new DHTrecord();
            DHTrecord.set(ID);
            new McastRecord();
            new NodeListener(ID,ID).start();
        }else {
            //If not original Node:
            try {
                socket = new DatagramSocket();
                address = InetAddress.getByName("localhost");
                while (true){
                    // assume random ID between 2 and 999
                    ID = (int)(Math.random()*998+2);
                    String msg = "Assuming " + Integer.toString(ID);
                    buffer = msg.getBytes();
                    packet = new DatagramPacket(buffer, buffer.length, address, 40001);
                    socket.send(packet);
                    buffer = new byte[256];
                    packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    msg = new String(packet.getData(), 0, packet.getLength());
                    if(msg.indexOf("Neighbors")>=0){
                        String[] N = msg.split("\\s+");
                        new McastRecord();
                        new NodeListener(Integer.parseInt(N[1]), Integer.parseInt(N[2])).start();
                        socket.close();
                        break;
                    }
                }
            }catch (Exception ex){ex.printStackTrace();}
        }

    }
}
