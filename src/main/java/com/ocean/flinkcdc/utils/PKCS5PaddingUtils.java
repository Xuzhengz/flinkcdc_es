package com.ocean.flinkcdc.utils;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.pqc.math.linearalgebra.ByteUtils;
import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;

public class PKCS5PaddingUtils {
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

    public static final String EPIDEMIC_KEY = "4d18850d763e8748ff2f8d83530e0cf2";


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


    public static byte[] encrypt_Ecb_Padding(byte[] key, byte[] data) throws Exception {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_PADDING, Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(data);
    }

    /**
     * @return java.lang.String
     * @Author gdq
     * @Description 解密
     * @Date 11:33 2021/1/15
     * @Param [cipherText]
     */
    public static String decrypt(String cipherText, String key) {
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
        } else {
            return cipherText;
        }
    }


    public static byte[] decrypt_Ecb_Padding(byte[] key, byte[] cipherText) throws Exception {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_PADDING, Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(cipherText);
    }

}
