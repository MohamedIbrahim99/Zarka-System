package LSM_tree;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LSMTreeHandler {
    private MemTable memTableA;
    private MemTable memTableB;
    private boolean isWorkingWithA;
    String folderPath;
    private Thread th1;
    private Thread th2;
    final int threshold = 12;

    private List<Map<String, Long>> hashIndecies;

    public LSMTreeHandler()
    {
        this.memTableA = new MemTable();
        this.memTableB = new MemTable();
        this.isWorkingWithA = true;
        this.hashIndecies = new ArrayList<>();
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
            System.out.println("added to A : " + String.valueOf(newKey) + newValue);
            if (size >= threshold && !th2.isAlive()) {
                System.out.println("Switch from A to B");
                memTableB = new MemTable();
                isWorkingWithA = false;
                // the start() method moves the thread to the active state
                th1.start();
            }
        }
        if (!isWorkingWithA) {
            int size = memTableB.add(newKey, newValue);
            System.out.println("added to B : " + String.valueOf(newKey) + newValue);
            if (size >= threshold && !th1.isAlive()) {
                System.out.println("Switch from B to A");
                memTableA = new MemTable();
                isWorkingWithA = true;
                // the start() method moves the thread to the active state
                th2.start();
            }
        }
    }

    // Search for the desired key
    public Map<String, String> get (int key)
    {
        Map<String, String> data;
        if (isWorkingWithA) {
            // check A first
            data = memTableA.get(key);
            if (data != null){
                return data;
            }
            // if it doesn't exist : check B
            data = memTableB.get(key);
            if (data != null){
                return data;
            }
        }
        else {
            // check B first
            data = memTableB.get(key);
            if (data != null){
                return data;
            }
            // if it doesn't exist : check A
            data = memTableA.get(key);
            if (data != null){
                return data;
            }
        }
        // if it doesn't exist until now : check SSTable
        return getFromSSTable();
    }

    private Map<String, String> getFromSSTable() {
        //Creating a File object for directory
        File directoryPath = new File(folderPath);
        //List of all files and directories
        File filesList[] = directoryPath.listFiles();
//        for(File file : filesList) {
//            System.out.println("File name: "+file.getName());
//            System.out.println("File path: "+file.getAbsolutePath());
//            System.out.println(" ---------------- ");
//        }
        return null;
    }


}
