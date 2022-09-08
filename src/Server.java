import ConsistentHashing.ConsistentHashing;
import LSM_tree.LSMTreeHandler;

import java.net.*;
import java.io.*;
import java.time.Instant;
import java.util.*;

public class Server
{
    private static String Address = "127.0.0.1";
    private static int NumberOfNodes;
    private static int DefaultPort;
    private static int NumberOfRunningNodes;
    private static int NumberOfVirtualNodes;
    private static int ReplicationFactor;
    private static int QuorumR;
    private static int QuorumW;
    private static int port;
    private static int Threshold;

    private static ConsistentHashing ch;
    private static LSMTreeHandler lsm;

    //initialize socket and input stream
    private Socket		 socket = null;
    private ServerSocket server = null;
    private DataInputStream in	 = null;
    private PrintWriter out	 = null;

    // constructor with port
    public Server(int port) {
        this.port = port;
        notifyOthers();
        ch = new ConsistentHashing(NumberOfRunningNodes,DefaultPort,NumberOfVirtualNodes);
        lsm = new LSMTreeHandler((port-DefaultPort)+"",Threshold);
        String line;
        int targetPort;
        String key;
        String value;
        String timeStamp;
        int writeCounter;

        while (true)
        {
        // starts server and waits for a connection
        try {
            server = new ServerSocket(port);
            System.out.println("Server started on port: "+port);

            System.out.println("Waiting for a client ...");

            socket = server.accept();
            System.out.println("Client accepted");

            // takes input from the client socket
            in = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);


            line = "";
            key="";
            value="";
            ArrayList<Map<String,String>> values;
            writeCounter = 0;

            line = in.readUTF();

            System.out.println(line);
            ch.showCircle();
            //New server is running
            if(line.contains("##")){
                NumberOfRunningNodes = Integer.parseInt(line.split(" ")[0]);
                ch.addNewNode(NumberOfRunningNodes);
                out.println(port+" Notified");
            }
            //Serve a client
            else{

                if(line.contains(" ")){//I am the coordinator
                    key = line.split(" ")[1];
                    targetPort = Integer.parseInt(ch.get(key));
                    System.out.println("targetPort "+targetPort);

                    if(line.split(" ")[0].equalsIgnoreCase("get")){
                        values = new ArrayList<>();
                        try {
                            if(targetPort==port){
                                Map<String,String> temp = lsm.get(key);
                                if(temp==null){
                                    temp =new HashMap<>();
                                    temp.put("key","notFound");
                                    temp.put("value","notFound");
                                    temp.put("timestamp","1662651580471");
                                }
                                values.add(temp);
                                for (int i = 1; i < ReplicationFactor; i++) {
                                    try {
                                        values.add(getValueRemote(( (port - 1 - DefaultPort+i) % NumberOfRunningNodes ) +DefaultPort+1,key));
                                    }catch (Exception e){
                                        System.out.println(e);
                                    }
                                }
                            }else{
                                for (int i = 0; i < ReplicationFactor; i++) {
                                    try {
                                        if(( (targetPort - 1 - DefaultPort+i) % NumberOfRunningNodes ) +DefaultPort+1 == port){
                                            Map<String,String> temp = lsm.get(key);
                                            if(temp==null){
                                                temp =new HashMap<>();
                                                temp.put("key","notFound");
                                                temp.put("value","notFound");
                                                temp.put("timestamp","1662651580471");
                                            }
                                            values.add(temp);
                                        }else{
                                            values.add(getValueRemote(( (targetPort - 1 - DefaultPort+i) % NumberOfRunningNodes ) +DefaultPort+1,key));
                                        }
                                    }catch (Exception e){
                                        System.out.println(e);
                                    }
                                }
                            }
                            if(values.size()>=QuorumR){
                                System.out.println("Success Reads: "+values.size());
                                value = values.get(0).get("value");
                                timeStamp = values.get(0).get("timestamp");
                                for (int i = 1; i < values.size(); i++) {
                                    if(new Date(Long.parseLong(values.get(i).get("timestamp"))).compareTo(new Date(Long.parseLong(timeStamp)))>0){
                                        value = values.get(i).get("value");
                                        timeStamp = values.get(i).get("timestamp");
                                    }
                                }
                                out.println(value+" "+NumberOfRunningNodes);
                                //Fix read
                            }else{
                                System.out.println("Success Reads: "+values.size());
                                out.println("Failed "+NumberOfRunningNodes);
                            }
                        }catch (Exception e){
                            out.println(e);
                        }
                    }

                    else if(line.split(" ")[0].equalsIgnoreCase("add")){
                        try {
                            value = line.split(" ")[2];
                            if(targetPort==port){
                                lsm.add(key,value);writeCounter++;

                                for (int i = 1; i < ReplicationFactor; i++) {
                                    try {
                                        System.out.println(addValueRemote(( (port - 1 - DefaultPort+i) % NumberOfRunningNodes ) +DefaultPort+1,key,value));
                                        writeCounter++;
                                    }catch (Exception e){
                                        System.out.println(e);
                                    }
                                }

                            }else{
                                for (int i = 0; i < ReplicationFactor; i++) {
                                    try {
                                        if(( (targetPort - 1 - DefaultPort+i) % NumberOfRunningNodes ) +DefaultPort+1 == port){
                                            lsm.add(key,value);
                                        }else{
                                            System.out.println(addValueRemote(( (targetPort - 1 - DefaultPort+i) % NumberOfRunningNodes ) +DefaultPort+1,key,value));
                                        }
                                        writeCounter++;

                                    }catch (Exception e){
                                        System.out.println(e);
                                    }
                                }

                            }
                            if(writeCounter>=QuorumW){
                                System.out.println("Success Writes: "+writeCounter);
                                out.println("Done "+NumberOfRunningNodes);
                            }else{
                                System.out.println("Success Writes: "+writeCounter);
                                out.println("Failed "+NumberOfRunningNodes);
                            }

                        }catch (Exception e){
                            out.println(e);
                        }

                    }else{
                        out.println("UndefinedCommand"+" "+NumberOfRunningNodes);
                    }

                }else{  // I am helper
                    if(line.contains(",")){//write
                        try
                        {
                            String[] tokens = line.split(",");
                            lsm.add(tokens[0],tokens[1]);
                            out.println("Written to "+port);
                        }
                        catch(Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                    else{//reading
                        Map<String,String> res = lsm.get(line);
                        System.out.println(res);
                        if(res==null){
                            out.println("notFound,notFound,1662651580471");
                        }else{
                            out.println(res.get("key")+","+res.get("value")+","+res.get("timestamp"));
                        }

                    }

                }
            }

            System.out.println("Closing connection\n*******************");

            // close connection
            socket.close();
            in.close();
            out.close();
            server.close();

        } catch (Exception e) {
            System.out.println(e);
            return;
        }
    }
    }
    //notify other nodes that I started
    private void notifyOthers(){
        Socket socket;
        DataOutputStream out;
        for (int i = 1; i <= NumberOfRunningNodes-1; i++) {
            try {
                socket = new Socket(Address, DefaultPort + i);
                out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF(NumberOfRunningNodes+" ##");

                System.out.println(new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine());
            }catch (Exception e){
                System.out.println(e);
            }
        }

    }

    private Map<String,String> getValueRemote(int port, String key) throws Exception {
        Socket socket;
        DataOutputStream out;
        socket = new Socket(Address, port);
        out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF(key);
        String response = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
        socket.close();
        out.close();
        String[] tokens = response.split(",");
        System.out.println(response);
        Map<String,String> m = new HashMap<>();
        m.put("key", tokens[0]);
        m.put("value", tokens[1]);
        m.put("timestamp", tokens[2]);
        return m;
    }

    private String addValueRemote(int port, String key,String value) throws Exception {
        Socket socket;
        DataOutputStream out;
        socket = new Socket(Address, port);
        out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF(key+","+value);
        String response = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
        socket.close();
        out.close();
        return response;
    }

//    private String getValue(String key){
//        String value="";
//        try
//        {
//            File file=new File(port+".txt");    //creates a new file instance
//            FileReader fr=new FileReader(file);   //reads the file
//            BufferedReader br=new BufferedReader(fr);  //creates a buffering character input stream
//            String l;
//            while((l=br.readLine())!=null)
//            {
//                if(l.split("#")[0].equals(key)){
//                    value = l.substring(l.indexOf('#')+1);
//                }
//            }
//            fr.close();    //closes the stream and release the resources
//        }
//        catch(Exception e)
//        {
//            e.printStackTrace();
//        }
//        return value;
//    }

//    private void addValue(String key,String value, String timeStamp) throws Exception {
//            File file=new File(port+".txt");    //creates a new file instance
//            FileWriter fw=new FileWriter(file,true);   //reads the file
//            BufferedWriter bw=new BufferedWriter(fw);  //creates a buffering character input stream
//            bw.append(key+"#"+value+"#"+timeStamp+"\n");
//            bw.flush();
//            bw.close();    //closes the stream and release the resources
//    }

    public static void main(String args[])
    {
        //Read configuration file
        Properties prop=new Properties();
        try{
            System.out.println("Read Configurations....");
            prop.load(new FileInputStream("config.properties"));
        }catch (Exception e){
            System.out.println("Can't find configuration file.");
            System.exit(1);
        }
        try{
            DefaultPort = Integer.parseInt(prop.getProperty("port"));
            NumberOfRunningNodes = Integer.parseInt(prop.getProperty("numberOfRunningNodes"));
            NumberOfNodes = Integer.parseInt(prop.getProperty("numberOfNodes"));
            NumberOfVirtualNodes = Integer.parseInt(prop.getProperty("numberOfVirtualNodes"));
            QuorumR = Integer.parseInt(prop.getProperty("quorumR"));
            QuorumW = Integer.parseInt(prop.getProperty("quorumW"));
            ReplicationFactor = Integer.parseInt(prop.getProperty("rf"));
            Threshold = Integer.parseInt(prop.getProperty("threshold"));
            prop.setProperty("numberOfRunningNodes",(NumberOfRunningNodes+1)+"");
            prop.store(new FileOutputStream("config.properties"),null);
        }catch (Exception e){
            System.out.println("port or numberOfRunningNodes is missing from configuration file");
            System.exit(1);
        }
        NumberOfRunningNodes++;
        Server server = new Server(DefaultPort+NumberOfRunningNodes);
    }
}
