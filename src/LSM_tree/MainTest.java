package LSM_tree;

import java.io.IOException;
import java.util.Map;

public class MainTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        LSMTreeHandler lsmTreeHandler = new LSMTreeHandler("temp",5);

        lsmTreeHandler.add("5","value5"); //
        lsmTreeHandler.add("44","value44"); //
        lsmTreeHandler.add("87","value87");
        lsmTreeHandler.add("90","value90");
        lsmTreeHandler.add("27","value27");
        lsmTreeHandler.add("2","value2");
        lsmTreeHandler.add("8","value8"); //
        lsmTreeHandler.add("17","value17");//
        lsmTreeHandler.add("58","value58");
        lsmTreeHandler.add("19","value19");
        lsmTreeHandler.add("1","value1");
        lsmTreeHandler.add("3","value3");
        lsmTreeHandler.add("100","value100");//
        lsmTreeHandler.add("20","value20");//
        lsmTreeHandler.add("9","value9");//

        lsmTreeHandler.add("22","value22");
        lsmTreeHandler.add("55","value55");
        lsmTreeHandler.add("67","value67");
        lsmTreeHandler.add("77","value77");
        lsmTreeHandler.add("16","value16");
        lsmTreeHandler.add("91","value91");
        lsmTreeHandler.add("88","value88");
        Thread.sleep(2000);
        lsmTreeHandler.add("13","value22");
        lsmTreeHandler.add("53","value55");
        lsmTreeHandler.add("22","value67");
        lsmTreeHandler.add("75","value77");
        lsmTreeHandler.add("70","value16");
        lsmTreeHandler.add("99","value91");
        lsmTreeHandler.add("86","value88");

        Thread.sleep(2000);

        Map<String, String> map= lsmTreeHandler.get("87");
        map= lsmTreeHandler.get("1");
        //System.out.println("get : " +map.get("key") + " , " + map.get("value") + " , " + map.get("timestamp"));
        map= lsmTreeHandler.get("100");
        //System.out.println("get : " +map.get("key") +" , " + map.get("value") +" , " + map.get("timestamp"));
        map= lsmTreeHandler.get("1");
        //System.out.println("get : " +map.get("key") +" , " + map.get("value") +" , " + map.get("timestamp"));
        map= lsmTreeHandler.get("5");
        //System.out.println("get : " +map.get("key") +" , " + map.get("value") +" , " + map.get("timestamp"));
        map= lsmTreeHandler.get("88");
        System.out.println("get : " + map.get("key") +" , " + map.get("value") +" , " + map.get("timestamp"));
      //  map= lsmTreeHandler.get(86);

//        System.out.println(lsmTreeHandler.get("z"));
//        lsmTreeHandler.add("z","9");
//        System.out.println(lsmTreeHandler.get("z"));
//        lsmTreeHandler.add("z","8");
//        System.out.println(lsmTreeHandler.get("z"));
//        lsmTreeHandler.add("z","12");
//        System.out.println(lsmTreeHandler.get("z"));
//        lsmTreeHandler.add("z","51");
//        System.out.println(lsmTreeHandler.get("z"));
//        lsmTreeHandler.add("z","59");
//        System.out.println(lsmTreeHandler.get("z"));

    }

}
