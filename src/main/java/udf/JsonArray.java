package udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/***
 * desc: 将json变成list集合，方便在hive中使用explode函数
 */

public class JsonArray extends UDF{

    /**
     * @param jsonStr  可以接收的参数格式 [{},{}]
     * @return          json数组转换后的list集合
     */
    public ArrayList<String> evaluate(Object jsonStr){
        //如果不是String类型，返回null
        if(!(jsonStr instanceof String)) return null;
        String jsonString = (String) jsonStr;
        return getJsonList(jsonString);
    }

    //将一个json数组转换成一个list集合返回    [{},{}]  ---->  list集合  list集合在hive中相当于是数组,就可以直接使用explode函数
    public ArrayList<String> getJsonList(String jsonArrayString){
        if(jsonArrayString == null || jsonArrayString.length() == 0) return null;

        JSONArray jsonArrayObj = null;
        try{
            jsonArrayObj = JSON.parseArray(jsonArrayString);
        }catch (Exception e){
            return null;
        }
        ArrayList<String> result = new ArrayList<String>();
        for(int i = 0 ; i<jsonArrayObj.size() ; i++){
            result.add(jsonArrayObj.get(i).toString());
        }
        return result;
    }

    public static void main(String[] args) {
        ArrayList<String> res = new JsonArray().evaluate("[{\"name\":\"jack\",\"age\":18},{\"name\":\"lele\",\"age\":20}]");
        System.out.println(res);
    }


}
