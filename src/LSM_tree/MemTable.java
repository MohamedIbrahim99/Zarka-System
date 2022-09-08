package LSM_tree;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MemTable {

    RedBlackTree redBlackTree;
    int size = 0;
    // Constructor for creating Red-Black tree
    public MemTable()
    {
        this.redBlackTree = new RedBlackTree(Integer.MIN_VALUE);
    }

    // Add a new node in the memTable
    public int add (int newKey, String newValue)
    {
        redBlackTree.insertNewNode(newKey, newValue);
        size += 1;
        return size;
    }

    // Search for the desired node in the memTable
    public Map<String, String> get (int key)
    {
        // if this key doesn't exist it returns null
        return redBlackTree.searchNode(key);
    }

    // Write to the disk as an SSTable file.
    // Since the tree is orderly, it can be written directly to the disk in sequence.
    public Map<String, Long> writeInSSTable (String path)
    {
        Map<String, Long> hashIndex = new HashMap<>();
        // Try block to check if exception occurs
        try {
            long timestamp = new Date().getTime();
            String fileName = path + File.separatorChar + String.valueOf(timestamp) + ".txt";
            // Create a FileWriter object to write in the file
            RandomAccessFile fWriter = new RandomAccessFile(fileName, "rw");

            hashIndex = redBlackTree.inorderTraversal(fWriter);

            // Closing the file writing connection
            fWriter.close();
        }
        
        // Catch block to handle if exception occurs
        catch (IOException e) {
            // Print the exception
            System.out.print(e.getMessage());
        }

        return hashIndex;
    }


}