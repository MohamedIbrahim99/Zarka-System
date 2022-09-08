package LSM_tree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class RedBlackTree2
{
    public Node root;//root node
    public RedBlackTree2()
    {
        super();
        root = null;
    }
    // node creating subclass
    class Node
    {
        int key;
        String value;
        String timestamp;
        Node left;
        Node right;
        char colour;
        Node parent;

        Node(int key, String value)
        {
            super();
            this.key = key; // only including data. not key
            this.value = value;
            this.timestamp = String.valueOf(new Date().getTime());
            this.left = null; // left subtree
            this.right = null; // right subtree
            this.colour = 'R'; // colour . either 'R' or 'B'
            this.parent = null; // required at time of rechecking.
        }
    }
    // this function performs left rotation
    Node rotateLeft(Node node)
    {
        Node x = node.right;
        Node y = x.left;
        x.left = node;
        node.right = y;
        node.parent = x; // parent resetting is also important.
        if(y!=null)
            y.parent = node;
        return(x);
    }
    //this function performs right rotation
    Node rotateRight(Node node)
    {
        Node x = node.left;
        Node y = x.right;
        x.right = node;
        node.left = y;
        node.parent = x;
        if(y!=null)
            y.parent = node;
        return(x);
    }


    // these are some flags.
    // Respective rotations are performed during traceback.
    // rotations are done if flags are true.
    boolean ll = false;
    boolean rr = false;
    boolean lr = false;
    boolean rl = false;
    // helper function for insertion. Actually this function performs all tasks in single pass only.
    Node insertHelp(Node root, int key, String newValue)
    {
        // f is true when RED RED conflict is there.
        boolean f=false;

        //recursive calls to insert at proper position according to BST properties.
        if(root==null)
            return(new Node(key, newValue));
        else if(key<root.key)
        {
            root.left = insertHelp(root.left, key, newValue);
            root.left.parent = root;
            if(root!=this.root)
            {
                if(root.colour=='R' && root.left.colour=='R')
                    f = true;
            }
        }
        else
        {
            root.right = insertHelp(root.right,key, newValue);
            root.right.parent = root;
            if(root!=this.root)
            {
                if(root.colour=='R' && root.right.colour=='R')
                    f = true;
            }
            // at the same time of insertion, we are also assigning parent nodes
            // also we are checking for RED RED conflicts
        }

        // now lets rotate.
        if(this.ll) // for left rotate.
        {
            root = rotateLeft(root);
            root.colour = 'B';
            root.left.colour = 'R';
            this.ll = false;
        }
        else if(this.rr) // for right rotate
        {
            root = rotateRight(root);
            root.colour = 'B';
            root.right.colour = 'R';
            this.rr = false;
        }
        else if(this.rl) // for right and then left
        {
            root.right = rotateRight(root.right);
            root.right.parent = root;
            root = rotateLeft(root);
            root.colour = 'B';
            root.left.colour = 'R';

            this.rl = false;
        }
        else if(this.lr) // for left and then right.
        {
            root.left = rotateLeft(root.left);
            root.left.parent = root;
            root = rotateRight(root);
            root.colour = 'B';
            root.right.colour = 'R';
            this.lr = false;
        }
        // when rotation and recolouring is done flags are reset.
        // Now lets take care of RED RED conflict
        if(f)
        {
            if(root.parent.right == root) // to check which child is the current node of its parent
            {
                if(root.parent.left==null || root.parent.left.colour=='B') // case when parent's sibling is black
                {// perform certaing rotation and recolouring. This will be done while backtracking. Hence setting up respective flags.
                    if(root.left!=null && root.left.colour=='R')
                        this.rl = true;
                    else if(root.right!=null && root.right.colour=='R')
                        this.ll = true;
                }
                else // case when parent's sibling is red
                {
                    root.parent.left.colour = 'B';
                    root.colour = 'B';
                    if(root.parent!=this.root)
                        root.parent.colour = 'R';
                }
            }
            else
            {
                if(root.parent.right==null || root.parent.right.colour=='B')
                {
                    if(root.left!=null && root.left.colour=='R')
                        this.rr = true;
                    else if(root.right!=null && root.right.colour=='R')
                        this.lr = true;
                }
                else
                {
                    root.parent.right.colour = 'B';
                    root.colour = 'B';
                    if(root.parent!=this.root)
                        root.parent.colour = 'R';
                }
            }
            f = false;
        }
        return(root);
    }

    // function to insert data into tree.
    public void insert(int key, String newValue)
    {
        if(this.root==null)
        {
            this.root = new Node(key, newValue);
            this.root.colour = 'B';
        }
        else
            this.root = insertHelp(this.root, key, newValue);
    }

    int count = 0;
    ///long offset = 0;
    final int numOfElements = 5;
    Map<String, Long> hashIndex = new HashMap<>();

    // helper function to print inorder traversal
    private Map<String, Long> inorderTraversalHelper(Node node, RandomAccessFile fWriter) throws IOException {
        if(node!=null)
        {
            inorderTraversalHelper(node.left, fWriter);

            // Content to be assigned to a file
            String text = String.valueOf(node.key) + ',' + node.value + ',' + node.timestamp;
            // assign the offset of the next write
            long offset = fWriter.length();
            // Writing into file
            fWriter.write(text.getBytes());
            fWriter.writeBytes(System.getProperty("line.separator"));
            // Printing the contents of a file
            System.out.println("Append in the SSTable : " + text);

            if (count % numOfElements == 0){
                hashIndex.put(String.valueOf(node.key), offset);
            }
            count++;
            ///offset += text.length();

            inorderTraversalHelper(node.right, fWriter);
        }
        return hashIndex;
    }
    //function to print inorder traversal
    public Map<String, Long> inorderTraversal(RandomAccessFile fWriter) throws IOException {
        return inorderTraversalHelper(this.root, fWriter);
    }

    // Create searchNode() method to get desired node from the Red-Black tree
    public Map<String, String> searchNode(int key)
    {
        return searchNode(this.root, key);
    }
    private Map<String, String> searchNode(Node node, int key)
    {
        Map<String, String> coordinates = null;
        while ((node != null) && coordinates == null)
        {
            int nodeValue = node.key;
            if (key < nodeValue)
                node = node.left;
            else if (key > nodeValue)
                node = node.right;
            else
            {
                coordinates = new HashMap<>();
                coordinates.put("value", node.value);
                coordinates.put("timestamp", node.timestamp);
                return coordinates;
            }
            coordinates = searchNode(node, key);
        }
        return coordinates;
    }

}
