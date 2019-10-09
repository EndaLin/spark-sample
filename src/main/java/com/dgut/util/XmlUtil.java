package com.dgut.util;

import com.alibaba.fastjson.JSONObject;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XmlUtil {
//    public static JSONObject xmlToJson(byte[] xml) throws JDOMException, IOException {
//        JSONObject json = new JSONObject();
//        InputStream is = new ByteArrayInputStream(xml);
//        SAXBuilder sb = new SAXBuilder();
//        Document doc = sb.build(is);
//        Element root = doc.getRootElement();
//        return iterateElement(root);
//    }
//
//    private static JSONObject iterateElement(Element root) {
//        List nodes = root.getChildren();
//        JSONObject json = new JSONObject();
//        for (Object obj : nodes) {
//            Element et = (Element) obj;
//            if (!et.getTextTrim().isEmpty()) {
//                Attribute attribute = (Attribute) et.getAttributes().get(0);
//                json.put(attribute.getValue(), et.getTextTrim());
//            }
//        }
//        return json;
//    }

    private final static Pattern PATTERN = Pattern.compile("<_f n=\"(.*?)\">(.*?)</_f>");
    public static JSONObject xmlToJson(String xml) {
        Matcher matcher = PATTERN.matcher(xml);
        JSONObject json = new JSONObject();
        while (matcher.find()) {
            json.put(matcher.group(1), matcher.group(2));
        }
        return json;
    }

}
