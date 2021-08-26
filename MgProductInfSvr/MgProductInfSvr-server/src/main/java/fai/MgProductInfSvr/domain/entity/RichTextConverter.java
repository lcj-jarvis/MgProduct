package fai.MgProductInfSvr.domain.entity;

import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextEntity;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextValObj;
import fai.comm.util.Errno;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.middleground.svrutil.exception.MgException;

public class RichTextConverter {

    public static String getKey(int type) {
        if(type == RichTextValObj.Type.REMARK) {
            return ProductBasicEntity.ProductInfo.REMARK;
        }

        return ProductBasicEntity.ProductInfo.REMARK + type;
    }

    public static int getType(String key) throws MgException {
        if(ProductBasicEntity.ProductInfo.REMARK.equals(key)) {
            return RichTextValObj.Type.REMARK;
        }

        String typeStr = key.replaceFirst(ProductBasicEntity.ProductInfo.REMARK, "");

        try {
            return Integer.valueOf(typeStr);
        }catch (NumberFormatException e) {
            throw new MgException("getType err;key=%s;", key);
        }
    }

    public static FaiList<Param> getRemarkList(Param data, boolean nullIsEmpty) {
        FaiList<Param> list = new FaiList<>();
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK1, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK2, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK3, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK4, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK5, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK6, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK7, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK8, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK9, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK10, nullIsEmpty));
        list.addNotNull(getRemark(data, ProductBasicEntity.ProductInfo.REMARK11, nullIsEmpty));

        return list;
    }

    public static Param getRemark(Param data, String key, boolean nullIsEmpty) {
        if(data == null) {
            throw new MgException(Errno.ARGS_ERROR, "getRemark args error;key=%s;nullIsEmpty=%s;", key, nullIsEmpty);
        }
        String remark = data.getString(key);
        if(remark == null && nullIsEmpty) {
            remark = "";
        }
        if(remark == null) {
            return null;
        }
        Param remarkInfo = new Param();
        remarkInfo.setString(MgRichTextEntity.Info.CONTENT, remark);
        remarkInfo.setInt(MgRichTextEntity.Info.TYPE, getType(key));
        remarkInfo.setInt(MgRichTextEntity.Info.BIZ, MgRichTextValObj.Biz.PRODUCT);
        remarkInfo.assign(data, ProductRelEntity.Info.PD_ID, MgRichTextEntity.Info.RLID);
        return remarkInfo;
    }
}
