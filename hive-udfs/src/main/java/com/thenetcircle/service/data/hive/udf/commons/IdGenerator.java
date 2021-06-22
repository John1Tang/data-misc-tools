package com.thenetcircle.service.data.hive.udf.commons;



import org.apache.commons.lang.math.RandomUtils;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public final class IdGenerator {

    private static final long BASE_TS = 1478016000;

    private static final char[] URL_SAFE_ENCODE_TABLE = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'
    };

    private static final byte[] DECODE_TABLE = {
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, 62, -1, 63, 52, 53, 54,
            55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4,
            5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, -1, -1, -1, -1, 63, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34,
            35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
    };

    private static final AtomicInteger counter = new AtomicInteger(0);

    private IdGenerator() {

    }

    public static String generate() {
        return generate(IDTypeEnum.DEFAULT);
    }

    public static String generate(IDTypeEnum idType) {
        return generate(idType.getCode());
    }

    public static String generate(int idType) {
        byte[] barr = new byte[48];

        // 5 for type
        int2bytes(idType, barr, 0, 5);

        // 30 for timestamp
        int ts = (int) (System.currentTimeMillis() / 1000 - BASE_TS);
        int2bytes(ts, barr, 5, 35);

        // 6 for auto increment
        int auto = counter.incrementAndGet() % 64;
        int2bytes(auto, barr, 35, 41);

        // 7 for random
        int rand = RandomUtils.nextInt(new Random(), 128);
        int2bytes(rand, barr, 41, 48);

        return bitMaptoString(barr);
    }

    public static IDTypeEnum getIDType(String id) {
        if (id == null || id.length() != 8) {
            return null;
        }
        try {
            int type = DECODE_TABLE[id.charAt(0)] >> 1;
            return IDTypeEnum.LOOKUP.get(type);
        } catch (Exception e) {
            return null;
        }
    }

    private static String bitMaptoString(byte[] bitmap) {
        assert bitmap.length % 6 == 0;
        int len = bitmap.length / 6;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            int baseIdx = i * 6;
            int idx = bitmap[baseIdx] << 5
                    | bitmap[baseIdx + 1] << 4
                    | bitmap[baseIdx + 2] << 3
                    | bitmap[baseIdx + 3] << 2
                    | bitmap[baseIdx + 4] << 1
                    | bitmap[baseIdx + 5];
            sb.append(URL_SAFE_ENCODE_TABLE[idx]);
        }
        return sb.toString();
    }

    private static void int2bytes(int val, byte[] arr, int st, int ed) {
        int len = ed - st;
        int value = val;
        for (int i = 1; i <= len; i++) {
            arr[ed - i] = (byte) (value & 0x1);
            value >>= 1;
        }
    }

    public static long toLongId(String id) {
        if (id != null && id.length() == 8) {
            byte[] bytes = id.getBytes();
            if (bytes.length == 8) {
                return ((((long) bytes[0] & 0xff) << 56)
                        | (((long) bytes[1] & 0xff) << 48)
                        | (((long) bytes[2] & 0xff) << 40)
                        | (((long) bytes[3] & 0xff) << 32)
                        | (((long) bytes[4] & 0xff) << 24)
                        | (((long) bytes[5] & 0xff) << 16)
                        | (((long) bytes[6] & 0xff) << 8)
                        | (((long) bytes[7] & 0xff)));
            }
        }
        return 0;
    }

    public static String fromLong(long id) {
        return String.valueOf((char) (id >> 56 & 0xff)) +
                (char) (id >> 48 & 0xff) +
                (char) (id >> 40 & 0xff) +
                (char) (id >> 32 & 0xff) +
                (char) (id >> 24 & 0xff) +
                (char) (id >> 16 & 0xff) +
                (char) (id >> 8 & 0xff) +
                (char) (id & 0xff);
    }
}
