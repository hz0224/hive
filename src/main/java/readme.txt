创建hive 永久函数：
create function default.get_userId as "udf.TransformUserId" using jar "hdfs://nameservice1/jar/hive-1.0-SNAPSHOT.jar";
create function default.get_userNumber as "udf.TransformUserNum" using jar "hdfs://nameservice1/jar/hive-1.0-SNAPSHOT.jar";
create function default.getjsonarray as "udf.JsonArray" using jar "hdfs://nameservice1/jar/hive-1.0-SNAPSHOT.jar";

删除函数：
    drop function 数据库.函数名;
注意：如果想要在第三方客户端上使用该函数，比如hue，需要重启下 hiveserver2服务。

impala同步hive自定义函数：
    invalidate metadata;




