package fai.MgProductBasicSvr.domain.common;

import fai.MgProductBasicSvr.domain.entity.ProductValObj;
import fai.comm.config.FaiConfig;
import fai.comm.util.Errno;
import fai.comm.util.Log;
import fai.comm.util.Str;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;

import java.util.Collection;
import java.util.HashSet;

public class MgProductCheck {
    public static void setIsDev(boolean isDev) {
        IS_DEV = isDev;
    }
    public static boolean isDev() {
        return IS_DEV;
    }

    public static class RequestLimit {
        public static <T> boolean checkWriteSize(int aid, Collection<T> list) {
            if(Utils.isEmptyList(list)) {
                Log.logErr(Errno.SIZE_LIMIT, "args error, rlPdIds is empty;aid=%d;list=%s;", aid, list);
                return false;
            }
            // 先去重再校验数量
            if(new HashSet<>(list).size() > WRITE_SIZE_LIMIT) {
                Log.logErr(Errno.SIZE_LIMIT, "args error, write size is too long;aid=%d;list size=%d;limit size=%d;", aid, list.size(), WRITE_SIZE_LIMIT);
                return false;
            }
            return true;
        }

        public static <T> boolean checkReadSize(int aid, Collection<T> list) {
            if(Utils.isEmptyList(list)) {
                throw new MgException(Errno.ARGS_ERROR, "args error, rlPdIds is empty;aid=%d;list=%s;", aid, list);
            }
            // 先去重再校验数量
            if(new HashSet<>(list).size() > READ_SIZE_LIMIT) {
                Log.logErr(Errno.SIZE_LIMIT, "args error, read size is too long;aid=%d;list size=%d;limit size=%d;", aid, list.size(), READ_SIZE_LIMIT);
                return false;
            }
            return true;
        }

        private static final int WRITE_SIZE_LIMIT = 100; // 写操作限制，最多可操作100条数据(包括修改、删除)
        private static final int READ_SIZE_LIMIT = 200; // 读操作限制，最多可读200个商品的数据
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

    private static boolean IS_DEV = false;
}
