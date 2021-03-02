package fai.MgProductStoreSvr.domain.comm;

import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordEntity;
import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordValObj;
import fai.comm.util.Param;

public class InOutStoreRecordArgCheck {

    /**
     * 校验optType
     */
    public static boolean isValidOptType(int optType){
        switch (optType) {
            case InOutStoreRecordValObj.OptType.IN:
            case InOutStoreRecordValObj.OptType.OUT:
                return true;
        }
        return false;
    }

    public static boolean isValidChangeCount(int changeCount){
        return changeCount >= 0;
    }

    public static boolean isValidRemark(Param info){
        if(info == null){
            return false;
        }
        String remark = info.getString(InOutStoreRecordEntity.Info.REMARK);
        return isValidRemark(remark);
    }
    /**
     * 校验remark
     */
    public static boolean isValidRemark(String remark){
        if(remark == null){
            return true;
        }
        int length = remark.length();
        return length >= InOutStoreRecordValObj.Limit.Remark.MIN_LEN && length <= InOutStoreRecordValObj.Limit.Remark.MAX_LEN;
    }

    public static boolean isValidRlOrderCode(Param info){
        if(info == null){
            return false;
        }
        String rlOrderCode = info.getString(InOutStoreRecordEntity.Info.RL_ORDER_CODE);
        return isValidRlOrderCode(rlOrderCode);
    }
    /**
     * 校验rlOrderCode
     */
    public static boolean isValidRlOrderCode(String rlOrderCode){
        if(rlOrderCode == null){
            return true;
        }
        int length = rlOrderCode.length();
        return length >= InOutStoreRecordValObj.Limit.RlOrderCode.MIN_LEN && length <= InOutStoreRecordValObj.Limit.RlOrderCode.MAX_LEN;
    }

    public static boolean isValidRlRefundId(Param info){
        if(info == null){
            return false;
        }
        String rlRefundId = info.getString(InOutStoreRecordEntity.Info.RL_REFUND_ID);
        return isValidRlRefundId(rlRefundId);
    }
    /**
     * 校验rlRefundId
     */
    public static boolean isValidRlRefundId(String rlRefundId){
        if(rlRefundId == null){
            return true;
        }
        int length = rlRefundId.length();
        return length >= InOutStoreRecordValObj.Limit.RlRefundId.MIN_LEN && length <= InOutStoreRecordValObj.Limit.RlRefundId.MAX_LEN;
    }
}
