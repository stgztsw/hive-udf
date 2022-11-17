package com.tangshiwei.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

public class URLCoderUtil {
    static BitSet dontNeedEncoding;
    private static final Logger log = LoggerFactory.getLogger(URLCoderUtil.class);
    private URLCoderUtil() {
    }

    static {
        dontNeedEncoding = new BitSet(128);
        int i;
        for (i = 'a'; i <= 'z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = 'A'; i <= 'Z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = '0'; i <= '9'; i++) {
            dontNeedEncoding.set(i);
        }
        dontNeedEncoding.set('+');
        dontNeedEncoding.set('-');
        dontNeedEncoding.set('_');
        dontNeedEncoding.set('.');
        dontNeedEncoding.set('*');
        dontNeedEncoding.set('%');
    }

    /**
     * 字符串是否经过了url encode
     *
     * @param text 字符串
     * @return true表示是
     */
    public static boolean hasEnCode(String text) {
        if (StringUtils.isBlank(text)) {
            return false;
        }
        for (int i = 0; i < text.length(); i++) {
            int c = text.charAt(i);
            if (!dontNeedEncoding.get(c)) {
                return false;
            }
            if (c == '%' && (i + 2) < text.length()) {
                // 判断是否符合urlEncode规范
                char c1 = text.charAt(++i);
                char c2 = text.charAt(++i);
                if (!isDigit16Char(c1) || !isDigit16Char(c2)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 对字符串s用charset方式编码
     *
     * @param s 需要编码的字符串
     * @return 编码后的字符串
     */
    public static String encode(String s) {
        return encode(s, StandardCharsets.UTF_8);
    }

    /**
     * 对字符串s用charset方式编码
     *
     * @param s       需要编码的字符串
     * @param charset 编码类型
     * @return 编码后的字符串
     */
    public static String encode(String s, Charset charset) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        try {
            return URLEncoder.encode(s, charset.name());
        } catch (Exception e) {
            log.error("exception：", e);
        }
        return null;
    }

    /**
     * 对已编码的字符串解码
     *
     * @param s 字符串
     * @return 解码后字符串
     */
    public static String decode(String s) {
        return decode(s, StandardCharsets.UTF_8);
    }

    /**
     * 对已编码的字符串解码
     *
     * @param s       字符串
     * @param charset 解码方式
     * @return 解码后字符串
     */
    public static String decode(String s, Charset charset) {
        try {
            return URLDecoder.decode(s, charset.name());
        } catch (Exception e) {
            log.error("exception：", e);
        }
        return null;
    }

    /**
     * 判断c是否是16进制的字符
     *
     * @param c 字符
     * @return true表示是
     */
    private static boolean isDigit16Char(char c) {
        return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F');
    }

}

