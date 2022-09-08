package LSM_tree;

import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

// Creating a node for the red-black tree. A node has left and right child, element and color of the node
class RedBlackNode
{
    RedBlackNode leftChild, rightChild;
    int key;
    String value;
    String timestamp;
    int color;

    // Constructor to set the value of a node having no left and right child
    public RedBlackNode(int key)
    {
        this(key,null, null, null );
    }

    // Constructor to set value of element, leftChild, rightChild and color
    public RedBlackNode(int key, String value, RedBlackNode leftChild, RedBlackNode rightChild)
    {
        this.key = key;
        this.value = value;
        this.timestamp = String.valueOf(new Date().getTime());
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        color = 1;
    }
}

// Create class CreateRedBlackTree for creating red-black tree
class RedBlackTree
{
    private static RedBlackNode nullNode;   //define null node
    private RedBlackNode current;   //define current node
    private RedBlackNode parent;    //define parent node
    private RedBlackNode header;   // define header node
    private RedBlackNode grand; //define grand node
    private RedBlackNode great; //define great node

    // Create two variables, i.e., RED and Black for color and the values of these variables are 0 and 1 respectively.
    static final int RED   = 0;
    static final int BLACK = 1;

    // Using static initializer for initializing null Node
    static
    {
        nullNode = new RedBlackNode(0);
        nullNode.leftChild = nullNode;
        nullNode.rightChild = nullNode;
    }


    // Constructor for creating header node
    public RedBlackTree(int header)
    {
        this.header = new RedBlackNode(header);
        this.header.leftChild = nullNode;
        this.header.rightChild = nullNode;
    }

    // create removeAll() for making the tree logically empty
    public void removeAll()
    {
        header.rightChild = nullNode;
    }

    //create method checkEmpty() to check whether the tree is empty or not
    public boolean checkEmpty()
    {
        return header.rightChild == nullNode;
    }

    // Create insertNewNode() method for adding a new node in the red black tree
    public void insertNewNode(int newKey, String newValue)
    {
        current = parent = grand = header;      //set header value to current, parent, and grand node
        nullNode.key = newKey;          //set newElement to the element of the null node

        // Repeat statements until the element of the current node will not equal to the value of the newElement
        while (current.key != newKey)
        {
            great = grand;
            grand = parent;
            parent = current;

            //if the value of the newElement is lesser than the current node element, the current node will point to the current left child else point to the current right child.
            current = newKey < current.key ? current.leftChild : current.rightChild;

            // Check whether both the children are RED or NOT. If both the children are RED change them by using handleColors() method
            if (current.leftChild.color == RED && current.rightChild.color == RED)
                handleColors( newKey );
        }

        // insertion of the new node will be fail if will already present in the tree
        if (current != nullNode)
            return;

        //create a node having no left and right child and pass it to the current node
        current = new RedBlackNode(newKey, newValue, nullNode, nullNode);
        System.out.println(String.valueOf(current.key));
        //connect the current node with the parent
        if (newKey < parent.key)
            parent.leftChild = current;
        else
            parent.rightChild = current;
        handleColors( newKey );
        inorderTraversal2(header.rightChild);
        System.out.println(nodesInTree());
    }

    //create handleColors() method to maintain the colors of Red-black tree nodes
    private void handleColors(int newElement)
    {
        // flip the colors of the node
        current.color = RED;    //make current node RED
        current.leftChild.color = BLACK;    //make leftChild BLACK
        current.rightChild.color = BLACK;   //make rightChild BLACK

        //check the color of the parent node
        if (parent.color == RED)
        {
            // perform rotation in case when the color of parent node is RED
            grand.color = RED;

            if (newElement < grand.key && grand.key != newElement && newElement < parent.key)
                parent = performRotation( newElement, grand );  // Start dbl rotate
            current = performRotation(newElement, great );
            current.color = BLACK;
        }

        // change the color of the root node with BLACK
        header.rightChild.color = BLACK;
    }

    //create performRotation() method to perform dbl rotation
    private RedBlackNode performRotation(int newElement, RedBlackNode parent)
    {
        //check whether the value of the newElement is lesser than the element of the parent node or not
        if(newElement < parent.key)
            //if true, perform the rotation with the left child and right child based on condition and set return value to the left child of the parent node
            return parent.leftChild = newElement < parent.leftChild.key ? rotationWithLeftChild(parent.leftChild) : rotationWithRightChild(parent.leftChild) ;
        else
            //if false, perform the rotation with the left child and right child based on condition and set return value to the right child of the parent node
            return parent.rightChild = newElement < parent.rightChild.key ? rotationWithLeftChild(parent.rightChild) : rotationWithRightChild(parent.rightChild);
    }

    //create rotationWithLeftChild() method  for rotating binary tree node with left child
    private RedBlackNode rotationWithLeftChild(RedBlackNode node2)
    {
        RedBlackNode node1 = node2.leftChild;
        node2.leftChild = node1.rightChild;
        node1.rightChild = node2;
        return node1;
    }

    // create rotationWithRightChild() method for rotating binary tree node with right child
    private RedBlackNode rotationWithRightChild(RedBlackNode node1)
    {
        RedBlackNode node2 = node1.rightChild;
        node1.rightChild = node2.leftChild;
        node2.leftChild = node1.leftChild;
        return node2;
    }

    // create nodesInTree() method for getting total number of nodes in a tree
    public int nodesInTree()
    {
        return nodesInTree(header.rightChild);
    }
    private int nodesInTree(RedBlackNode node)
    {
        if (node == nullNode)
            return 0;
        else
        {
            int size = 1;
            size = size + nodesInTree(node.leftChild);
            size = size + nodesInTree(node.rightChild);
            return size;
        }
    }
    // Create searchNode() method to get desired node from the Red-Black tree
    public Map<String, String> searchNode(int key)
    {
        return searchNode(header.rightChild, key);
    }
    private Map<String, String> searchNode(RedBlackNode node, int key)
    {
        System.out.println(nodesInTree());
        Map<String, String> coordinates = null;
        while ((node != nullNode) && coordinates == null)
        {
            int nodeValue = node.key;
            if (key < nodeValue)
                node = node.leftChild;
            else if (key > nodeValue)
                node = node.rightChild;
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

    int count = 0;
    ///long offset = 0;
    final int numOfElements = 5;
    Map<String, Long> hashIndex = new HashMap<>();
    // Create inorderTraversal() method to perform inorder traversal
    public Map<String, Long> inorderTraversal(RandomAccessFile fWriter) throws IOException {
        inorderTraversal(header.rightChild, fWriter);
        return hashIndex;
    }
    private Map<String, Long> inorderTraversal(RedBlackNode node, RandomAccessFile fWriter) throws IOException {
        if (node != nullNode)
        {
            inorderTraversal(node.leftChild, fWriter);

            // Content to be assigned to a file
            String text = String.valueOf(node.key) + ',' + node.value + ',' + node.timestamp;
            // assign the offset of the next write
            long offset = fWriter.length();
            // Writing into file
            fWriter.write(text.getBytes());
            // Printing the contents of a file
            System.out.println("Append in the SSTable : " + text);

            if (count % numOfElements == 0){
                hashIndex.put(String.valueOf(node.key), offset);
            }
            count++;
            ///offset += text.length();

            inorderTraversal(node.rightChild, fWriter);
        }
        return hashIndex;
    }

    private Map<String, Long> inorderTraversal2(RedBlackNode node) {
        if (node != nullNode)
        {
            inorderTraversal2(node.leftChild);

            System.out.print(node.key + " - ");
            System.out.println();

            inorderTraversal2(node.rightChild);
        }
        return hashIndex;
    }

}