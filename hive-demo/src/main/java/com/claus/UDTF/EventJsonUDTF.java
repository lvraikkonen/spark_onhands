package com.claus.UDTF;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        // 定义 UDTF 返回值名称和类型
        List<String> fieldName = new ArrayList<String>();
        List<ObjectInspector> fieldType = new ArrayList<ObjectInspector>();

        fieldName.add("event_name");
        fieldName.add("event_json");

        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldType);
    }

    public void process(Object[] objects) throws HiveException {
        String input = objects[0].toString();
        if (StringUtils.isBlank(input)) {
            return;
        } else {
            JSONObject ja = new JSONObject(input);
            if (ja == null) {
                return;
            }

            for (int i=0; i<ja.length(); i++) {
                String[] result = new String[2];
                try {
                    result[0] = ja.getJSONObject(String.valueOf(i)).getString("en"); // event_name
                    result[1] = ja.getString(String.valueOf(i)); // 取出每一个事件整体
                } catch (JSONException e) {
                    continue;
                }

                forward(result);
            }
        }
    }

    public void close() throws HiveException {

    }

}
