package fai.MgProductBasicSvr.domain.common;

import fai.MgProductBasicSvr.domain.entity.ProductValObj;
import fai.comm.util.Log;
import fai.comm.util.Str;
import fai.mgproduct.comm.Util;

import java.util.Collection;

public class MgProductCheck {
    public static class RequestLimit {
        public static <T> boolean checkWriteSize(int aid, Collection<T> list) {
            if(Util.isEmptyList(list)) {
                Log.logErr("args error, rlPdIds is empty;aid=%d;list=%s;", aid, list);
                return false;
            }
            if(list.size() > WRITE_SIZE_LIMIT) {
                Log.logErr("args error, write size is too long;aid=%d;list size=%d;limit size=%d;", aid, list.size(), WRITE_SIZE_LIMIT);
                return false;
            }
            return true;
        }
        public static final int WRITE_SIZE_LIMIT = 100; // 写操作限制，最多可操作100条数据(包括修改、删除)
    }

    public static boolean checkProductName(String name) {
        if(Str.isEmpty(name)) {
            return false;
        }
        if(name.length() > ProductValObj.Limit.NAME_MAXLEN) {
            return false;
        }
        return true;
    }

}
