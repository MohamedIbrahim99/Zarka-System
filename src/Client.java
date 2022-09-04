import java.net.*;
import java.io.*;
import java.util.Properties;

public class Client
{
    private static String Address = "127.0.0.1";
    private static int NumberOfNodes;
    private static int DefaultPort;


    // initialize socket and output stream
    private Socket socket;
    private DataOutputStream out;
    public void run(String query,int port)
    {
        // establish a connection and send the query
        try
        {
            System.out.println("Connecting to server with port "+port+" .....");
            socket = new Socket(Address, port);
            System.out.println("Connected");

            // sends output to the socket
            out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF(query);

            // print received results
            System.out.println(new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine());
            System.out.println("*******************");
        }
        catch(Exception e)
        {
            System.out.println(e);
            return;
        }



        // close the connection
        try
        {
            out.close();
            socket.close();
        }
        catch(IOException i)
        {
            System.out.println(i);
        }
    }

    public static void main(String args[]) {
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
            NumberOfNodes = Integer.parseInt(prop.getProperty("numberOfNodes"));
        }catch (Exception e){
            System.out.println("port or numberOfNodes is missing from configuration file");
            System.exit(1);
        }
        System.out.println("Client is up");

        // string to read message from input
        String line = "";
        //port number to send query to
        int port;

        //Create client object
        Client client = new Client();

        //instialize input stream
        DataInputStream input =  new DataInputStream(System.in);;

        // keep reading until "exit | e" is input
        while (true)
        {
            try
            {
                line = input.readLine();
                if((line.equals("exit") | line.equals("e"))) break;
                port = (int)( Math.random() * NumberOfNodes + (DefaultPort+1) );
                client.run(line,port);
            }
            catch(IOException i)
            {
                System.out.println(i);
            }
        }
        try{
            input.close();
        }catch (Exception e){
            System.out.println(e);
        }

    }
}
