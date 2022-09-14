package com.ocean.flinkcdc;

import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import cn.hutool.poi.excel.ExcelWriter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.pqc.math.linearalgebra.ByteUtils;
import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.security.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Sme4 {
    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private static final String ENCODING = "UTF-8";
    public static final String ALGORITHM_NAME = "SM4";
    // 加密算法/分组加密模式/分组填充方式
    // PKCS5Padding-以8个字节为一组进行分组加密
    // 定义分组加密模式使用：PKCS5Padding
    public static final String ALGORITHM_NAME_ECB_PADDING = "SM4/ECB/PKCS5Padding";
    // 128-32位16进制；256-64位16进制
    public static final int DEFAULT_KEY_SIZE = 128;

    private static final String EPIDEMIC_KEY = "4d18850d763e8748ff2f8d83530e0cf2";

    public static String generateKey() throws Exception {
        return  ByteUtils.toHexString(generateKey(DEFAULT_KEY_SIZE));
    }

    public static byte[] generateKey(int keySize) throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance(ALGORITHM_NAME, BouncyCastleProvider.PROVIDER_NAME);
        kg.init(keySize, new SecureRandom());
        return kg.generateKey().getEncoded();
    }

    private static Cipher generateEcbCipher(String algorithmName, int mode, byte[] key) throws Exception {
        Cipher cipher = Cipher.getInstance(algorithmName, BouncyCastleProvider.PROVIDER_NAME);
        Key sm4Key = new SecretKeySpec(key, ALGORITHM_NAME);
        cipher.init(mode, sm4Key);
        return cipher;
    }


    /**
     * @Author gdq
     * @Description 加密
     * @Date 11:33 2021/1/15
     * @Param [paramStr]
     * @return java.lang.String
     */
    public static String encrypt(String paramStr,String key) {
        try {
            if(paramStr != null && !"".equals(paramStr)){
                String cipherText = "";
                // 16进制字符串--&gt;byte[]
                byte[] keyData = ByteUtils.fromHexString(key);
                // String--&gt;byte[]
                byte[] srcData = paramStr.getBytes(ENCODING);
                // 加密后的数组
                byte[] cipherArray = encrypt_Ecb_Padding(keyData, srcData);
                // byte[]--&gt;hexString
                cipherText = ByteUtils.toHexString(cipherArray);
                return cipherText;
            }else{
                return paramStr;
            }
        } catch (Exception e) {
            return paramStr;
        }
    }


    public static byte[] encrypt_Ecb_Padding(byte[] key, byte[] data) throws Exception {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_PADDING, Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(data);
    }

    /**
     * @Author gdq
     * @Description 解密
     * @Date 11:33 2021/1/15
     * @Param [cipherText]
     * @return java.lang.String
     */
    public static String decrypt(String cipherText,String key) {
        if (cipherText != null && !"".equals(cipherText)) {
            // 用于接收解密后的字符串
            String decryptStr = "";
            // hexString--&gt;byte[]
            byte[] keyData = ByteUtils.fromHexString(key);
            // hexString--&gt;byte[]
            byte[] cipherData = ByteUtils.fromHexString(cipherText);
            // 解密
            byte[] srcData = new byte[0];
            try {
                srcData = decrypt_Ecb_Padding(keyData, cipherData);
                // byte[]--&gt;String
                decryptStr = new String(srcData, ENCODING);
                return decryptStr;
            } catch (Exception e) {
                return cipherText;
            }
        }else{
            return cipherText;
        }
    }


    public static byte[] decrypt_Ecb_Padding(byte[] key, byte[] cipherText) throws Exception {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_PADDING, Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(cipherText);
    }


    public static boolean verifyEcb(String hexKey, String cipherText, String paramStr) throws Exception {
        // 用于接收校验结果
        boolean flag = false;
        // hexString--&gt;byte[]
        byte[] keyData = ByteUtils.fromHexString(hexKey);
        // 将16进制字符串转换成数组
        byte[] cipherData = ByteUtils.fromHexString(cipherText);
        // 解密
        byte[] decryptData = decrypt_Ecb_Padding(keyData, cipherData);
        // 将原字符串转换成byte[]
        byte[] srcData = paramStr.getBytes(ENCODING);
        // 判断2个数组是否一致
        flag = Arrays.equals(decryptData, srcData);
        return flag;
    }

    public static List<Map<String, Object>> getFiled(File file) {
        List<Map<String, Object>> readAll = new ArrayList<>();
        ExcelReader reader = ExcelUtil.getReader(file);
        readAll.addAll(reader.readAll());
        return readAll;
    }

    public static Map<String, Object> processBeforeSender(Map<String, Object> record) {
        for(String s : record.keySet()){
            if(record.get(s) != null){
                record.put(s, encrypt(record.get(s).toString(),EPIDEMIC_KEY));
            }
        }
        return record;
    }


    public static void main(String[] args) throws Exception {
//        File file = new File("C:\\Users\\bigbi\\Desktop\\bzdz_all.xlsx");
//        List<Map<String, Object>> r = new ArrayList<>();
//        List<Map<String, Object>> maps = getFiled(file);
//        for(Map<String, Object> map : maps){
//            r.add(processBeforeSender(map));
//        }
//        // 通过工具类创建writer
//        ExcelWriter writer = ExcelUtil.getWriter("C:\\Users\\bigbi\\Desktop\\bzdz_all_enrypt.xlsx");
//// 一次性写出内容，使用默认样式，强制输出标题
//        writer.write(r, true);
//// 关闭writer，释放内存
//        writer.close();

        System.out.println(decrypt("4a6f8a352288cac4520f665eee16269ae3fc70d57d2f3d7d290d207b93d33acb79a0617895560a61953a0ac9316b9b10", EPIDEMIC_KEY));

    }

}
