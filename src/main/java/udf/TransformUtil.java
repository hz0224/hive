package udf;

public class TransformUtil {

    //获取userNumber
    public static String getUserNumber(Long userId){
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

    //获取userId
    public static  Long getUserId(String userNumber){
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

}
