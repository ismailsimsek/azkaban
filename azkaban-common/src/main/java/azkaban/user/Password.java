/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.user;

//import java.security.SecureRandom;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

public class Password {
    public static final String PBKDF2_ALGORITHM = "PBKDF2WithHmacSHA1";

    public static final int HASH_BYTES = 24;
    public static final int PBKDF2_ITERATIONS = 1000;

    private final String password;
    private final String salt;

    public Password(String password, String salt) {
        this.password = password;
        this.salt = salt;
    }

    public String getPassword() {
        return this.password;
    }

    public String getSalt() {
        return this.salt;
    }

    @Override
    public String toString() {
        return "Password " + this.password + "Salt " + this.salt;
    }


    public boolean validatePassword(String rawPassword) {
        try {
            String testHash = createHash(rawPassword.toCharArray(), this.salt.getBytes(StandardCharsets.UTF_8));
            return (testHash.equals(this.password));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    public String hashPassword(String rawPassword, String salt) throws NoSuchAlgorithmException, InvalidKeySpecException {
        try {
            return createHash(rawPassword.toCharArray(), salt.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return "";
        }
    }

    private String createHash(char[] rawPassword, byte[] salt) throws NoSuchAlgorithmException, InvalidKeySpecException {
        byte[] hash = pbkdf2(rawPassword, salt, PBKDF2_ITERATIONS, HASH_BYTES);
        return PBKDF2_ITERATIONS + ":" + toHex(salt) + ":" + toHex(hash);
    }

    private byte[] pbkdf2(char[] password, byte[] salt, int iterations, int bytes)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        PBEKeySpec spec = new PBEKeySpec(password, salt, iterations, bytes * 8);
        SecretKeyFactory skf = SecretKeyFactory.getInstance(PBKDF2_ALGORITHM);
        return skf.generateSecret(spec).getEncoded();
    }

    private String toHex(byte[] array) {
        BigInteger bi = new BigInteger(1, array);
        String hex = bi.toString(16);
        int paddingLength = (array.length * 2) - hex.length();
        if (paddingLength > 0)
            return String.format("%0" + paddingLength + "d", 0) + hex;
        else
            return hex;
    }

}
