package com.tangshiwei.udtf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

@org.apache.hadoop.hive.ql.exec.Description(name = "explode_json_array", value = "_FUNC_(a) - separates the elements of json array a into multiple rows;")
public class GenericUDTFExplodeJSONArray extends GenericUDTF {
    private static final Logger log = LoggerFactory.getLogger(GenericUDTFExplodeJSONArray.class);

    /**
     * 初始化
     *
     * @param argOIs
     * @return
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        log.debug("initializing GenericUDTFExplodeJSONArray");
        //TODO 1 约束函数传入参数的个数
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentLengthException("explode_json_array Function can only input one argument...");
        }

        //TODO 2 约束函数传入参数的类型
        String typeName = argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName();
        if (!"string".equals(typeName)) {
            log.error("Got" + typeName + "instead of string...");
            throw new UDFArgumentTypeException(0, "explode_json_array Function，The type of the first parameter can only be String...," + "but \"" + typeName + "\" is found");
        }

        //TODO 3 约束函数返回值的类型
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("item"); //列别名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        log.debug("done initializing GenericUDTFExplodeJSONArray");
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 处理逻辑
     *
     * @param args object array of arguments
     */
    @Override
    public void process(Object[] args) throws HiveException {
        String[] result = new String[1];
        String[] nullCols = {null};

        if (args[0] == null || args.length != 1) {

            forward(nullCols);
            return;
        }
        try {
            String jsonArrayStr = args[0].toString();
            JSONArray jsonArray = new JSONArray(jsonArrayStr);

            for (int i = 0; i < jsonArray.length(); i++) {
                String jsonStr = jsonArray.getString(i);
                result[0] = jsonStr;
                forward(result);
            }
        }   catch (Exception e){
            log.error("Have Drity Data...");
            forward(nullCols);
        }


    }

    @Override
    public void close() throws HiveException {

    }
}

