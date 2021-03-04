package fai.mgproduct.comm;

import fai.comm.util.FaiList;

public class Util {
    public static <T> boolean isEmptyList(FaiList<T> list) {
        return list == null || list.isEmpty();
    }
}
