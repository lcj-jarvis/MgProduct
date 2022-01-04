package fai.MgProductInfSvr.domain.comm.gfw;

import fai.MgProductInfSvr.domain.entity.RichTextConverter;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextEntity;
import fai.comm.util.FaiList;
import fai.comm.util.Gfw;
import fai.comm.util.Param;
import fai.comm.util.Str;

public class YkGfw extends MgGfw {
    private static YkGfw ykGfw = new YkGfw();

    private YkGfw() {
        this.type = Gfw.Type.YK_PRODUCT;
    }

    public static YkGfw getInstance() {
        return ykGfw;
    }

    @Override
    protected void writeRichLog(int aid, int siteId, int rlPdId, FaiList<Param> list) {
        FaiList<Param> richGfwList = new FaiList<>();
        for(Param rich : list) {
            String val = rich.getString(MgRichTextEntity.Info.CONTENT);
            if(Str.isEmpty(val)) {
                continue;
            }
            int richType = rich.getInt(MgRichTextEntity.Info.TYPE);
            String key = RichTextConverter.getKey(richType);
            Param gfwData = new Param();
            gfwData.setInt(Gfw.Info.AID, aid);
            gfwData.setInt(Gfw.Info.ID, rlPdId);
            gfwData.setInt(Gfw.Info.FOLDER_ID, siteId);
            gfwData.setString(Gfw.Info.KEY, key);
            gfwData.setString(Gfw.Info.VALUE, val);

            richGfwList.add(gfwData);
        }
        Gfw.writeGfwLog(this.type, richGfwList);
    }
}
