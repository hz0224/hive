package udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Set;

/***
 * Created by Liu HangZhou on 2020/04/10
 * desc: 将json变成list集合，方便在hive中使用explode函数
 *
 * 常见的json格式
 * {"key1":value1,"key2":value2}       json
 * [{},{}]                             json数组
 * {"key1":[{},{}],"key2":[{},{}]}      json
 */

public class JsonArray extends UDF{

    /**
     * 这里之所以参数类型设置为Object是因为hive中的null和java中的null是不一样的.
     * hive中的null不是String类型,如果传进来的参数是null，是直接会报错.所以这里将参数类型设置为Object
     * 在代码里根据参数的具体类型做具体处理.
     * @param jsonStr  可以接收的参数格式 ① [{},{}] ② {"key1":[{},{}],"key2":[{},{}]}
     * @return          json数组转换后的list集合
     */
    public ArrayList<String> evaluate(Object jsonStr){

        //如果不是String类型，返回null
        if(!(jsonStr instanceof String)) return null;
        String jsonString = (String) jsonStr;

        if(jsonString.startsWith("[")){
            return getJsonList(jsonString);
        }else if(jsonString.startsWith("{")){
            return getAllJsonList(jsonString);
        }else{
            return null;
        }
    }

    //将一个json数组转换成一个list集合返回    [{},{}]  ---->  list集合  list集合在hive中相当于是数组,就可以直接使用explode函数
    public ArrayList<String> getJsonList(String jsonArrayString){
        if(jsonArrayString.length() == 0 || jsonArrayString == null) return null;

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

    //将一个json {"key":[{},{}],"key":[{},{}]}     转换成    [{},{},{}] 这样的list集合返回.
    public ArrayList<String> getAllJsonList(String jsonString){

        JSONObject jsonObject = null;
        try{
            jsonObject = JSON.parseObject(jsonString);
        }catch (Exception e){
            return null;
        }
        //获取json对象中所有的key
        Set<String> keys = jsonObject.keySet();
        ArrayList<Object> jsonObjList = new ArrayList<Object>();
        //获取所有的json对象(格式上是一个json数组)
        for (String key : keys) {
            jsonObjList.add(jsonObject.get(key));
        }
        ArrayList<String> result = new ArrayList<String>();
        //将所有的对象合并到一个list集合中返回
        for (Object jsonObj : jsonObjList) {
            ArrayList<String> jsonList = getJsonList(jsonObj.toString());
            result.addAll(jsonList);
        }

        return result;
    }


}
