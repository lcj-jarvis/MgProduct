package fai.MgProductStoreSvr.domain.entity;


import fai.comm.util.Parser;

import java.util.Calendar;

/**
 * 出入库记录
 */
public class InOutStoreRecordValObj {
    /**
     * 限制
     */
    public static final class Limit{
        public static final class Remark {
            public static final int MIN_LEN = 0; // 最小长度
            public static final int MAX_LEN = 100; // 最大长度
        }
        public static final class RlOrderCode {
            public static final int MIN_LEN = 0; // 最小长度
            public static final int MAX_LEN = 32; // 最大长度
        }
        public static final class RlRefundId {
            public static final int MIN_LEN = 0; // 最小长度
            public static final int MAX_LEN = 32; // 最大长度
        }
    }
    /**
     * 默认值
     */
    public static final class Default{
        public static final int SORT = 1;
    }

    /**
     * 标志位
     */
    public static final class FLag{
        public static final int NOT_CHANGE_COUNT = 0x1;   // 不改变总库存 - 退货入库等，有入库记录只更新remainCount不更新count
        public static final int RESET_PRICE = 0x2;   // 重置过成本
    }

    /**
     * 操作类型
     */
    public static final class OptType{
        public static final int IN = 1;  // 入库操作
        public static final int OUT = 2;   // 出库操作

        /**
         * 计算数量
         * @param optType
         * @param count 原数量
         * @param changeCount 变动数量
         * @return 计算后的数量
         */
        public static int computeCount(int optType, int count, int changeCount){
            switch (optType){
                case OptType.IN:
                {
                    count += changeCount;
                    break;
                }
                case OptType.OUT:
                {
                    count -= changeCount;
                    break;
                }
                default:
                    throw new RuntimeException("optType err;optType="+ optType);
            }
            return count;
        }
    }
    public static final class Number{
        public static String genNumber(String yyMMdd, int ioStoreRecId){
            return yyMMdd+String.format("%04d", ioStoreRecId);
        }

        public static String genNumber(Calendar calendar, int ioStoreRecId){
            String yyMMdd = Parser.parseString(calendar, "yyMMdd");
            return genNumber(yyMMdd, ioStoreRecId);
        }
    }

    public static final class Status {
        public static final int DEL = -1; // 删除状态
        public static final int DEFAULT = 0; // 默认状态
    }

    /**
     * {@link fai.comm.util.SearchArg}
     * 相关限制
     */
    public static final class SearchArg{
        public static final class Limit{
            public static final int MAX = 100;
        }
    }
}
