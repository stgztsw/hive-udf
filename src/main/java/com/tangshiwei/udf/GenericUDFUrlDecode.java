package com.tangshiwei.udf;

import com.tangshiwei.udtf.GenericUDTFExplodeJSONArray;
import com.tangshiwei.utils.URLCoderUtil;
import lombok.SneakyThrows;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;


@org.apache.hadoop.hive.ql.exec.Description(name = "url_decode", value = "_FUNC_(arguments) - Utility class for HTML form decoding. " +
        "This class contains static methods for decoding a String from the application/x-www-form-urlencoded MIME format.\n" +
        "The conversion process is the reverse of that used by the URLEncoder class. " +
        "It is assumed that all characters in the encoded string are one of the following: \"a\" through \"z\", \"A\" through \"Z\", \"0\" through \"9\", and \"-\", \"_\", \".\", and \"*\". " +
        "The character \"%\" is allowed but is interpreted as the start of a special escaped sequence. ")
public class GenericUDFUrlDecode extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFUrlDecode.class);
    /**
     * 判断传入参数类型
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //TODO 1.检查入参个数
        if (arguments.length!=1){
            throw new UDFArgumentLengthException("Please input only one arg");
        }
        //TODO 2.检查入参类型
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
        ){
            throw new UDFArgumentTypeException(1,"Please input string arg");
        }
        //TODO 3.约束返回值类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }
    

    /**
     * 处理逻辑
     * @param arguments
     * @return
     * @throws HiveException
     */
    @SneakyThrows
    @Override
    public Object evaluate(DeferredObject[] arguments)  {
        Object o = arguments[0].get();
        if (o==null){
            return "";
        }else {
            try {
            return URLCoderUtil.hasEnCode(o.toString()) ? URLDecoder.decode(o.toString(), StandardCharsets.UTF_8.name()) : o.toString();
//            return URLDecoder.decode(o.toString(), StandardCharsets.UTF_8.name()) ;
        }catch (RuntimeException e){
                LOG.error("decode error");
                return null;
            }
        }
    }

    /**
     * 帮助信息
     * @param strings
     * @return
     */
    @Override
    public String getDisplayString(String[] strings) {
        return getStandardDisplayString("GenericUDFDecode",strings);
    }

}