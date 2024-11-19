import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");


        ServerSocket serverSocket = null;
        int port = 6379;
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
                ClientHandler clientHandler = new ClientHandler(clientSocket);
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

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
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
                    String command = null, content= null;

                    // Read the command
                    for (int i = 0; i < arrayLength; i++) {
                        reader.readLine();  // Skip the $n line
                        String part = reader.readLine();  // Read the actual content
                        if (i == 0) {
                            command = part.toUpperCase();
                        }
                        else if(i == arrayLength - 1) {
                            content = part;
                        }
                    }

                    // Handle commands
                    if ("PING".equals(command)) {
                        clientOutputStream.write("+PONG\r\n".getBytes());
                        clientOutputStream.flush();
                    }
                    else if("ECHO".equals(command)) {
                        String output  = "$" + content.length() + "\r\n" +content+ "\r\n";
                        clientOutputStream.write(output.getBytes());
                        clientOutputStream.flush();
                    }
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