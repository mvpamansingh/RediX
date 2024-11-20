import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");


        ServerSocket serverSocket = null;
        int port = 6379;


      ConcurrentHashMap<String, String> dataStore = new ConcurrentHashMap<>();
        try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);

         // clientSocket = serverSocket.accept();

            while(true) {

                // Wait for connection from client.once client connected return object of conencted client
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected");

                // Create a new ClientHandler for each client and start it in a new thread
                ClientHandler clientHandler = new ClientHandler(clientSocket ,dataStore );
                new Thread(clientHandler).start();
            }


        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (serverSocket != null) {
             serverSocket.close();
            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }


  }
}

class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final ConcurrentHashMap<String, String> dataStore;

    public ClientHandler(Socket socket, ConcurrentHashMap<String, String> dataStore) {
        this.clientSocket = socket;
        this.dataStore = dataStore;
    }

    @Override
    public void run() {
        try {
            OutputStream clientOutputStream = clientSocket.getOutputStream();
            InputStream clientInputStream = clientSocket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientInputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                // Handle RESP protocol format
                if (line.startsWith("*")) {  // Array length
                    int arrayLength = Integer.parseInt(line.substring(1));
                    String[] parts = new String[arrayLength];

                    String command = null, content= null;

                    // Read the command

                    for(int i=0;i<arrayLength;i++){
                        reader.readLine();
                        parts[i]= reader.readLine();
                    }
                    command = parts[0].toUpperCase();
//                    for (int i = 0; i < arrayLength; i++) {
//                        reader.readLine();  // Skip the $n line
//                        String part = reader.readLine();  // Read the actual content
//                        if (i == 0) {
//                            command = part.toUpperCase();
//                        }
//                        else if(i == arrayLength - 1) {
//                            content = part;
//                        }
//                    }

                    switch(command){
                        case "PING":{
                            clientOutputStream.write("+PONG\r\n".getBytes());
                            //clientOutputStream.flush();
                            break;
                        }

                        case "ECHO":{
                            String output  = "$" + parts[1].length() + "\r\n" +parts[1]+ "\r\n";
                            clientOutputStream.write(output.getBytes());
                            break;
                        }
                        case "SET": {
                            if (parts.length >= 3) {
                                dataStore.put(parts[1], parts[2]);
                                clientOutputStream.write("+OK\r\n".getBytes());
                            }
                            break;
                        }
                        case "GET": {
                            if (parts.length >= 2) {
                                String value = dataStore.get(parts[1]);
                                String output;
                                if (value != null) {
                                    output = "$" + value.length() + "\r\n" + value + "\r\n";
                                } else {
                                    output = "$-1\r\n";
                                }
                                clientOutputStream.write(output.getBytes());
                            }
                            break;
                        }
                    }

                    clientOutputStream.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}