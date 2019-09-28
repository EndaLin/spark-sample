package com.dgut.spark_code;

import com.alibaba.fastjson.JSONObject;
import com.dgut.util.XmlUtil;
import org.apache.spark.sql.api.java.UDF1;

public class XmlToJson implements UDF1<String, String> {
    @Override
    public String call(String xml) throws Exception {
        JSONObject json = XmlUtil.xmlToJson(xml.replaceAll("\r|\n","").getBytes());
        System.out.println(json);
        return json.toJSONString();
    }
}
