package LSM_tree;

import java.util.Map;

public class MainTest {

    public static void main(String[] args) {
        LSMTreeHandler lsmTreeHandler = new LSMTreeHandler();

        lsmTreeHandler.add(5,"value5"); //
        lsmTreeHandler.add(44,"value44"); //
        lsmTreeHandler.add(87,"value87");
        lsmTreeHandler.add(90,"value90");
        lsmTreeHandler.add(27,"value27");
        lsmTreeHandler.add(2,"value2");
        lsmTreeHandler.add(8,"value8"); //
        lsmTreeHandler.add(17,"value17");//
        lsmTreeHandler.add(58,"value58");
        lsmTreeHandler.add(19,"value19");
        lsmTreeHandler.add(1,"value1");
        lsmTreeHandler.add(3,"value3");
        lsmTreeHandler.add(100,"value100");//
        lsmTreeHandler.add(20,"value20");//
        lsmTreeHandler.add(9,"value9");//

        Map<String, String> map= lsmTreeHandler.get(87);
        System.out.println(map.get("key") + " , " + map.get("value") + " , " + map.get("timestamp"));
        map= lsmTreeHandler.get(100);
        System.out.println(map.get("key") +" , " + map.get("value") +" , " + map.get("timestamp"));
        map= lsmTreeHandler.get(1);
        System.out.println(map.get("key") +" , " + map.get("value") +" , " + map.get("timestamp"));
        map= lsmTreeHandler.get(5);
        System.out.println(map.get("key") +" , " + map.get("value") +" , " + map.get("timestamp"));




    }

}
