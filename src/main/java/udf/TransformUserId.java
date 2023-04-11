package udf;

import org.apache.hadoop.hive.ql.exec.UDF;


public class TransformUserId extends UDF{

    public  Long evaluate(String userNumber){
        if(userNumber == null) return null;
        userNumber = userNumber.toUpperCase();
        if (userNumber.startsWith("AZ") || userNumber.startsWith("GZ")) {
            String userIdStr = userNumber.substring(2)
                    //老规则,奇数位为字母,用户id <= 523000 是按照老的
                    .replaceAll("A", "0")
                    .replaceAll("C", "1")
                    .replaceAll("E", "2")
                    .replaceAll("G", "3")
                    .replaceAll("I", "4")
                    .replaceAll("K", "5")
                    .replaceAll("M", "6")
                    .replaceAll("O", "7")
                    .replaceAll("Q", "8")
                    .replaceAll("S", "9")
                    //新规则,偶数位为字母,用户id > 523000 的用户id是按新的
                    .replaceAll("A", "0")
                    .replaceAll("V", "1")
                    .replaceAll("E", "2")
                    .replaceAll("T", "3")
                    .replaceAll("X", "4")
                    .replaceAll("K", "5")
                    .replaceAll("M", "6")
                    .replaceAll("H", "7")
                    .replaceAll("N", "8")
                    .replaceAll("S", "9");
            try{
                return Long.parseLong(userIdStr);
            }catch (Exception e){
                return null;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println(new TransformUserId().evaluate("1K4T2A4V"));
    }

}
