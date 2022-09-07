package ConsistentHashing;

public class Node {
    public String name ;
    public String portName ;

    public Node(String name,String portName) {
        this.name = name;
        this.portName = portName;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPortName() {
        return portName;
    }

    public void setPortName(String portName) {
        this.portName = portName;
    }

    public String getName() {
        return name;
    }
}
