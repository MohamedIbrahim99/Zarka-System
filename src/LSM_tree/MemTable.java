package LSM_tree;

public class MemTable {

    RedBlackTree redBlackTree;
    // Constructor for creating Red-Black tree
    public MemTable()
    {
        this.redBlackTree = new RedBlackTree(Integer.MIN_VALUE);
    }

    // Add a new node in the memTable
    public void add (int newKey, String newValue)
    {
        redBlackTree.insertNewNode(newKey, newValue);
    }

    // Search for the desired node in the memTable
    public String get (int key)
    {
        // if this key doesn't exist it returns null
        return redBlackTree.searchNode(key);
    }

    // Write to the disk as an SSTable file.
    // Since the tree is orderly, it can be written directly to the disk in sequence.
    public void writeInSSTable()
    {

    }

    //
    private void traverse()
    {

    }

}