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


      ConcurrentHashMap<String, KeyValue> dataStore = new ConcurrentHashMap<>();
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
    private final ConcurrentHashMap<String, KeyValue> dataStore;

    public ClientHandler(Socket socket, ConcurrentHashMap<String, KeyValue> dataStore) {
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
                if (line.startsWith("*")) {
                    int arrayLength = Integer.parseInt(line.substring(1));
                    String[] parts = new String[arrayLength];

                    for(int i = 0; i < arrayLength; i++) {
                        reader.readLine(); // Skip the $n line
                        parts[i] = reader.readLine();
                    }
                    String command = parts[0].toUpperCase();

                    switch(command) {
                        case "PING": {
                            clientOutputStream.write("+PONG\r\n".getBytes());
                            break;
                        }
                        case "ECHO": {
                            String output = "$" + parts[1].length() + "\r\n" + parts[1] + "\r\n";
                            clientOutputStream.write(output.getBytes());
                            break;
                        }
                        case "SET": {
                            if (parts.length >= 3) {
                                String key = parts[1];
                                String value = parts[2];
                                Long expiryTime = null;

                                // Check for PX argument
                                for (int i = 3; i < parts.length - 1; i++) {
                                    if (parts[i].equalsIgnoreCase("px")) {
                                        try {
                                            long milliseconds = Long.parseLong(parts[i + 1]);
                                            expiryTime = System.currentTimeMillis() + milliseconds;
                                        } catch (NumberFormatException e) {
                                            System.out.println("Invalid PX value: " + parts[i + 1]);
                                        }
                                        break;
                                    }
                                }

                                dataStore.put(key, new KeyValue(value, expiryTime));
                                clientOutputStream.write("+OK\r\n".getBytes());
                            }
                            break;
                        }
                        case "GET": {
                            if (parts.length >= 2) {
                                String key = parts[1];
                                KeyValue keyValue = dataStore.get(key);
                                String output;

                                if (keyValue != null && !keyValue.isExpired()) {
                                    output = "$" + keyValue.getValue().length() + "\r\n" + keyValue.getValue() + "\r\n";
                                } else {
                                    if (keyValue != null && keyValue.isExpired()) {
                                        dataStore.remove(key);
                                    }
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


class KeyValue {
    private final String value;
    private final Long expiryTime; // null means no expiry

    public KeyValue(String value, Long expiryTime) {
        this.value = value;
        this.expiryTime = expiryTime;
    }

    public String getValue() {
        return value;
    }

    public boolean isExpired() {
        return expiryTime != null && System.currentTimeMillis() > expiryTime;
    }
}