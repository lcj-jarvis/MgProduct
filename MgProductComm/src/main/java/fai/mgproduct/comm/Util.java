package fai.mgproduct.comm;

import fai.comm.util.FaiList;

import java.util.Collection;

public class Util {
    public static <T> boolean isEmptyList(Collection<T> list) {
        return list == null || list.isEmpty();
    }
}
