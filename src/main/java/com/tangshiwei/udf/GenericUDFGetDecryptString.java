package com.tangshiwei.udf;

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
import sun.misc.BASE64Decoder;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.spec.AlgorithmParameterSpec;

@org.apache.hadoop.hive.ql.exec.Description(name = "get_decrypt_string", value = "_FUNC_(arguments) - Creates a map with the given key/value pairs ")
public class GenericUDFGetDecryptString extends GenericUDF {

    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFGetDecryptString.class);
    private static final String key = "TfngF12e";
    private static SecretKey securekey;
    private static AlgorithmParameterSpec iv;

    /**
     * 判断传入参数类型
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //TODO 1.检查入参个数
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("Please input only one arg");
        }
        //TODO 2.检查入参类型
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(1, "Please input string arg");
        }
        //TODO 3.约束返回值类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    /**
     * 处理逻辑
     *
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if (o == null) {
            return null;
        } else {
            return new String(getDecryptString(String.valueOf(o)), StandardCharsets.ISO_8859_1);
        }
    }

    public static byte[] getDecryptString(String str) {

        try {
            DESKeySpec desKey = new DESKeySpec(key.getBytes());
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
            //密钥
            securekey = keyFactory.generateSecret(desKey);
            //向量
            iv = new IvParameterSpec(key.getBytes());
        } catch (Exception e) {
            LOG.error("DESKeySpec initialization failed");
            e.printStackTrace();
        }

        BASE64Decoder decoder = new BASE64Decoder();
        try {
            byte[] bytes = decoder.decodeBuffer(str);
            Cipher cipher = Cipher.getInstance("DES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, securekey, iv);
            //解密
            byte[] doFial = cipher.doFinal(bytes);

            return doFial;

        } catch (Exception e) {
            LOG.error("Decrypt Error");
            throw new RuntimeException(e);
        }
    }


    /**
     * 帮助信息
     *
     * @param strings
     * @return
     */
    @Override
    public String getDisplayString(String[] strings) {
        return getStandardDisplayString("GenericUDFGetDecryptString", strings);
    }


}
