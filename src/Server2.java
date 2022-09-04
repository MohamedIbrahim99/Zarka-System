import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

public class Server2
{
    private static String Address = "127.0.0.1";
    private static int NumberOfNodes;
    private static int DefaultPort;
    private static int NumberOfRunningNodes;

    //initialize socket and input stream
    private Socket		 socket = null;
    private ServerSocket server = null;
    private DataInputStream in	 = null;
    private PrintWriter out	 = null;

    // constructor with port
    public Server2(int port) {
        notifyOthers();
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

            String line = "";

            line = in.readUTF();
            if(line.contains("##")){
                NumberOfRunningNodes = Integer.parseInt(line.split(" ")[0]);
                out.println(port+" Notified");
            }else{
                System.out.println(line);
                out.println("recived");
            }

            System.out.println("Closing connection\n*******************");

            // close connection
            socket.close();
            in.close();
            out.close();
            server.close();

        } catch (Exception e) {
            System.out.println(e);
        }
    }
    }
    //notify other nodes that I started
    private void notifyOthers(){
        Socket socket;
        DataOutputStream out;
        for (int i = 1; i <= NumberOfRunningNodes; i++) {
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
            prop.setProperty("numberOfRunningNodes",(NumberOfRunningNodes+1)+"");
            prop.store(new FileOutputStream("config.properties"),null);
        }catch (Exception e){
            System.out.println("port or numberOfRunningNodes is missing from configuration file");
            System.exit(1);
        }

        Server2 server = new Server2(DefaultPort+NumberOfRunningNodes+1);
    }
}
