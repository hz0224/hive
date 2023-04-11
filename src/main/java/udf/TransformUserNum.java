package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.math.BigInteger;

public class TransformUserNum extends UDF {

    public String evaluate(Long userId){
        if(userId == null) return null;
        String userIdStr = String.valueOf(userId);

        String userNum = null;
        if(userId <= 523000){
            userNum = userIdStr.replaceAll("0", "A")
                    .replaceAll("1", "C")
                    .replaceAll("2", "E")
                    .replaceAll("3", "G")
                    .replaceAll("4", "I")
                    .replaceAll("5", "K")
                    .replaceAll("6", "M")
                    .replaceAll("7", "O")
                    .replaceAll("8", "Q")
                    .replaceAll("9", "S");
        }else{
            userNum = userIdStr.replaceAll("0", "A")
                    .replaceAll("1", "V")
                    .replaceAll("2", "E")
                    .replaceAll("3", "T")
                    .replaceAll("4", "X")
                    .replaceAll("5", "K")
                    .replaceAll("6", "M")
                    .replaceAll("7", "H")
                    .replaceAll("8", "N")
                    .replaceAll("9", "S");
        }
        String flag = userId < 1000101? "GZ" : "AZ";

        StringBuilder sb = new StringBuilder(flag);
        for (int i = 0; i < userIdStr.length(); i++) {
            if(i % 2 ==0){
                sb.append(userIdStr.charAt(i));
            }else{
                sb.append(userNum.charAt(i));
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(new TransformUserNum().evaluate(15432041L));
    }



}
