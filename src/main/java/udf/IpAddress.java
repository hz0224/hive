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
        if(sessionState != null){
            HiveConf hiveConf = sessionState.getConf();
            /**
             * create function default.get_address as "udf.IpAddress" using jar "hdfs://nameservice1/jar/hive-1.0-SNAPSHOT-jar-with-dependencies.jar", file "hdfs://nameservice1/jar/ip2region.xdb";
             * 当我们创建了一个如上的永久函数时，每次使用这个函数都会将 hdfs上这两个文件下载到每个节点的本地： /tmp/sessionId/xxx文件路径下，sessionId是每次不一样的。可以使用 list jars , list files 命令查看得到。
             * 因此我们需要在udf里动态的获取到该路径然后读取文件。
             */
            String resourcePath = hiveConf.getVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR); //hive.downloaded.resources.dir
            String filePath = resourcePath + "/" + "ip2region.xdb";
            byte[] bytes = Searcher.loadContentFromFile(filePath);
            this.searcher =  Searcher.newWithBuffer(bytes);
        }
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
