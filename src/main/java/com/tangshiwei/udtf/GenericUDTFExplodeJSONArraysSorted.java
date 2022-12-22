package com.tangshiwei.udtf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@org.apache.hadoop.hive.ql.exec.Description(name = "explode_json_arrays_sorted", value = "_FUNC_(a) - " +
        "separates the elements of json array a into multiple rows; If columns of different lengths are inputed, " +
        "the number of rows in the longest column will returned." +
        "The others will return \'{}\';")
public class GenericUDTFExplodeJSONArraysSorted extends GenericUDTF {
    private static final Logger log = LoggerFactory.getLogger(GenericUDTFExplodeJSONArraysSorted.class);

    /**
     * 初始化
     * @param argOIs 入参个数
     * @return
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        log.debug("initializing GenericUDTFExplodeJSONArraysSorted");
        //TODO 1 约束函数传入参数的个数,至少一个
        if (argOIs.getAllStructFieldRefs().size() < 1) {
            throw new UDFArgumentLengthException("Function explode_json_arrays_sorted has to input at least one column...");
        }

        //TODO 2 约束函数传入参数的类型
        List<? extends StructField> structFieldRefs = argOIs.getAllStructFieldRefs();

        for (int i = 0; i < structFieldRefs.size(); i++) {
            String typeName = structFieldRefs.get(0).getFieldObjectInspector().getTypeName();
            if (!"string".equals(typeName)) {
                log.error("Got" + typeName + "instead of string...");
                throw new UDFArgumentTypeException(i, "explode_json_arrays_sorted Function，The type of the"+i+
                        "th" + "arg can only be String...," + "but \"" + typeName + "\" is found");
            }
        }

        //TODO 3 约束函数返回值的类型
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        for (int i = 0; i < structFieldRefs.size(); i++) {
            fieldNames.add("col_" + (i+1)); //列别名
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);  // 列类型
        }

        log.debug("done initializing GenericUDTFExplodeJSONArraysSorted");
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 处理逻辑
     *
     * @param args object array of arguments
     */
    @Override
    public void process(Object[] args) throws HiveException {
        String[] result = new String[args.length];

        ArrayList<JSONArray> jsonArrayList = new ArrayList<>();
        int[] ints = new int[args.length];

        for (int i = 0; i < args.length; i++) {
            JSONArray jsonArray;
            try {
                jsonArray = new JSONArray(args[i].toString());
            } catch (JSONException jsonException) {
                log.error("Have Drity Data...，input data["+args[i].toString()+"] ;");
                jsonArray = new JSONArray("[]");
            } catch (NullPointerException ne) {
                log.error(ne.getClass().getName()+",Input null data...;");
                jsonArray = new JSONArray("[]");
            }
            jsonArrayList.add(jsonArray);
            ints[i] = jsonArray.length();
        }

        int[] sortInts = Arrays.stream(ints).sorted().toArray();
        int maxLengthJsonArray = sortInts[sortInts.length - 1];
        for (int j = 0; j < maxLengthJsonArray; j++) {
            for (int i = 0; i < jsonArrayList.size(); i++) {
                String json;
                // 遍历取第i个jsaonArray的第j个json
                try {
                    json = jsonArrayList.get(i).get(j).toString();
                } catch (JSONException je) {
                    json = "{}";
                }
                result[i] = json;
            }
            forward(result);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}

