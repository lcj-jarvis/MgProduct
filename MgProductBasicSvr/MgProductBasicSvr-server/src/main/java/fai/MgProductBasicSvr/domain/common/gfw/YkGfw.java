package fai.MgProductBasicSvr.domain.common.gfw;


import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.comm.util.*;
import fai.middleground.svrutil.misc.Utils;

public class YkGfw extends MgGfw {
    private static YkGfw ykGfw = new YkGfw();

    private YkGfw() {
        this.type = Gfw.Type.YK_PRODUCT;
    }

    public static YkGfw getInstance() {
        return ykGfw;
    }

    @Override
    protected FaiList<Param> buildLogData(int aid, int siteId, Param info) {
        if(Str.isEmpty(info) || siteId < 0) {
            return null;
        }
        String name = info.getString(ProductEntity.Info.NAME);
        Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
        if(name == null || pdId == null) {
            Log.logErr("build gfw data error;aid=%s;siteId=%s;info=%s;", aid, siteId, info);
            return null;
        }

        Param gfwData = new Param();
        gfwData.setInt(Gfw.Info.AID, aid);
        gfwData.setInt(Gfw.Info.ID, pdId);
        gfwData.setInt(Gfw.Info.FOLDER_ID, siteId);
        gfwData.setString(Gfw.Info.KEY, ProductEntity.Info.NAME);
        gfwData.setString(Gfw.Info.VALUE, name);

        // 临时存放unionPriId, 为了查出pdId对应的rlPdId
        Integer unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
        if(unionPriId == null) {
            unionPriId = info.getInt(ProductEntity.Info.SOURCE_UNIONPRIID);
        }
        if(unionPriId == null) {
            Log.logErr("build gfw data error, unionPriId is null;aid=%s;siteId=%s;info=%s;", aid, siteId, info);
            return null;
        }
        gfwData.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);

        return Utils.asFaiList(gfwData);
    }
}
