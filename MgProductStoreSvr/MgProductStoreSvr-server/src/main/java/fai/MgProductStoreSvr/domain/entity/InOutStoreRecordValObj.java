package fai.MgProductStoreSvr.domain.entity;


import fai.comm.util.Errno;
import fai.comm.util.Log;
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
}
