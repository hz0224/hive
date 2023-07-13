package udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.lionsoul.ip2region.xdb.Searcher;

import java.io.IOException;

public class IpAddress extends UDF {
    Searcher searcher = null;

    //反射创建该对象时就会执行无参构造
    public IpAddress() throws IOException {
        SessionState sessionState = SessionState.get();
        String filePath = null;

        /**
         * create function default.get_address as "udf.IpAddress" using jar "hdfs://nameservice1/jar/hive-1.0-SNAPSHOT-jar-with-dependencies.jar", file "hdfs://nameservice1/jar/ip2region.xdb";
         * 当我们创建了一个如上的永久函数时，每次使用这个函数都会将 hdfs上这两个文件下载到每个节点的本地，不过基于yarn的复杂查询和基于session的普通查询，这些文件在本地的存放路径不同。
         * 因此我们需要在udf里动态的获取到该路径然后读取文件。
         */
        if(sessionState != null){//此时说明执行的只是简单的session查询 比如 ： select default.get_address('1.180.4.8') ，不会上yarn运行
            HiveConf hiveConf = sessionState.getConf();
            /**
             * 基于session的简单查询： 会下载到 /tmp/sessionId/xxx文件路径下，sessionId是每次不一样的。可以使用 list jars , list files 命令查看得到。
             */
            String resourcePath = hiveConf.getVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR); //hive.downloaded.resources.dir
            filePath = resourcePath + "/" + "ip2region.xdb";
        }else{  //此时说明执行的是复杂的查询，已经上了yarn。
            HiveConf hiveConf = new HiveConf();
            /**
             * 基于yarn的复杂查询：会下载到 /data/cm6/yarn/nm/usercache/root/appcache/application_1689190630163_2804/container_e95_1689190630163_2804_01_000002/ip2region.xdb
             */
            filePath = hiveConf.getResource("ip2region.xdb").getPath();
        }
        byte[] bytes = Searcher.loadContentFromFile(filePath);
        this.searcher =  Searcher.newWithBuffer(bytes);
    }

    public  String evaluate(String ip) throws IOException {
        JSONObject res = new JSONObject();
        if(ip == null) return null;
        try{
            String region = searcher.search(ip);
            String[] data = region.split("\\|");
            String country = data[0];
            String area = data[1];
            String province = data[2];
            String city = data[3];
            String isr = data[4];
            res.put("country", country);
            res.put("area",area);
            res.put("province", province);
            res.put("city", city);
            res.put("isr", isr);
        }catch (Exception e){
            return null;
        }
        return res.toJSONString();
    }
}
