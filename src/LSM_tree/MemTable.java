package LSM_tree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemTable {

    RedBlackTree2 redBlackTree;
    int size = 0;
    // Constructor for creating Red-Black tree
    public MemTable()
    {
        this.redBlackTree = new RedBlackTree2();
    }

    // Add a new node in the memTable
    public int add (String newKey, String newValue)
    {
        redBlackTree.insert(newKey, newValue);
        size += 1;
        return size;
    }

    // Search for the desired node in the memTable
    public Map<String, String> get (String key)
    {
        // if this key doesn't exist it returns null
        return redBlackTree.searchNode(key);
    }

    Map<String, Long> hashIndex;
    // Write to the disk as an SSTable file.
    // Since the tree is orderly, it can be written directly to the disk in sequence.
    public void writeInSSTable (String path)
    {
        Map<String, Long> hashIndex = new HashMap<>();
        // Try block to check if exception occurs
        try {
            long timestamp = new Date().getTime();
            String fileName = path + File.separatorChar + String.valueOf(timestamp) + ".txt";
            // Create a RandomAccessFile object to write in the file
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
        this.hashIndex = hashIndex;
        //return hashIndex;
    }


}