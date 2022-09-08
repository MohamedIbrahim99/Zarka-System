package LSM_tree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class LSMTreeHandler {
    private MemTable memTableA;
    private MemTable memTableB;
    private boolean isWorkingWithA;
    String folderPath;
    private Thread th1;
    private Thread th2;
    final int threshold = 12;

    private static List<Map<String, Long>> hashIndecies;

    public LSMTreeHandler()
    {
        this.memTableA = new MemTable();
        this.memTableB = new MemTable();
        this.isWorkingWithA = true;
        hashIndecies = new ArrayList<>();
        ///// folder path
        this.folderPath = "./temp";
        File directory = new File(folderPath);
        if (! directory.exists()){
            directory.mkdir();
        }
        try {
            Files.createDirectories(Paths.get("./temp"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // creating an object of the class Thread using Thread(Runnable r)
        this.th1 = new Thread(() -> memTableA.writeInSSTable(folderPath));
        this.th2 = new Thread(() -> memTableB.writeInSSTable(folderPath));
    }

    // Add a new node in the memTable
    public void add (int newKey, String newValue)
    {
        if (isWorkingWithA) {
            int size = memTableA.add(newKey, newValue);
            System.out.println("added to A : " + String.valueOf(newKey)+ " , " + newValue);
            if (size >= threshold && !th2.isAlive()) {
                System.out.println("Switch from A to B");
                if (!th2.getState().equals(Thread.State.NEW))
                    hashIndecies.add(memTableB.hashIndex);
                memTableB = new MemTable();
                isWorkingWithA = false;
                // the start() method moves the thread to the active state
                th1.start();
            }
        }
        else if (!isWorkingWithA) {
            int size = memTableB.add(newKey, newValue);
            System.out.println("added to B : " + String.valueOf(newKey)+ " , " + newValue);
            if (size >= threshold && !th1.isAlive()) {
                System.out.println("Switch from B to A");
                hashIndecies.add(memTableA.hashIndex);
                memTableA = new MemTable();
                isWorkingWithA = true;
                // the start() method moves the thread to the active state
                th2.start();
            }
        }
    }

    // Search for the desired key
    public Map<String, String> get (int key) throws IOException {
        Map<String, String> data;
        if (isWorkingWithA) {
            // check A first
            data = memTableA.get(key);
            if (data != null){
                System.out.println("get from A : " + data.get("key")+ " , " + data.get("value"));
                return data;
            }
            // if it doesn't exist : check B
            data = memTableB.get(key);
            if (data != null){
                System.out.println("get from B : " + data.get("key")+ " , " + data.get("value"));
                return data;
            }
        }
        else {
            // check B first
            data = memTableB.get(key);
            if (data != null){
                System.out.println("get from B : " + data.get("key")+ " , " + data.get("value"));
                return data;
            }
            // if it doesn't exist : check A
            data = memTableA.get(key);
            if (data != null){
                System.out.println("get from A : " + data.get("key")+ " , " + data.get("value"));
                return data;
            }
        }
        // if it doesn't exist until now : check SSTable
        return getFromSSTable(key);
    }

    private Map<String, String> getFromSSTable(int key) throws IOException {
        Map<String, String> data = new HashMap<>();
        //Creating a File object for directory
        File directoryPath = new File(folderPath);
        //List of all files and directories
        File filesList[] = directoryPath.listFiles();
        Arrays.sort(filesList);
        for(int i = filesList.length-1; i >= 0; i--) {
            File file = filesList[i];
            System.out.println("File name: "+file.getName());
            // Create a RandomAccessFile object to read with offset in the file
            RandomAccessFile reader = new RandomAccessFile(file, "rw");
            String line = reader.readLine();
            while (line != null){
                String[] tokens = line.split(",");
                if (Integer.parseInt(tokens[0]) == key){
                    data.put("key", tokens[0]);
                    data.put("value", tokens[1]);
                    data.put("timestamp", tokens[2]);
                    System.out.println("read from SSTable : " + tokens[0]+ " , " + tokens[1]);
                    return data;
                }
                line = reader.readLine();
            }
            reader.close();
        }
        return null;
    }

}
