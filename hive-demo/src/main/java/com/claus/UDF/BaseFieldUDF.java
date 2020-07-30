package com.claus.UDF;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {

    public String evaluate(String line, String key) {
        String[] log = line.split("\\|");

        //
        if (log.length != 2 || StringUtils.isBlank(log[1].trim())) {
            return "";
        }
        JSONObject json = new JSONObject(log[1].trim());

        String result = "";

        // 根据key值取值
        if ("st".equals(key)) {
            result = log[0].trim();
        } else if ("et".equals(key)) {
            if (json.has("et")) {
                result = json.getString("et");
            }
        } else {
            JSONObject cm = json.getJSONObject("cm");
            if (cm.has(key)) {
                result = cm.getString(key);
            }
        }
        return result;
    }

    public static void main(String[] args) {
        String line = "13513515135|{\"cm\":{\"ln\":-45, \"os\": \"macOS\"}}";
        String result = new BaseFieldUDF().evaluate(line, "os");
        System.out.println(result);
    }
}
