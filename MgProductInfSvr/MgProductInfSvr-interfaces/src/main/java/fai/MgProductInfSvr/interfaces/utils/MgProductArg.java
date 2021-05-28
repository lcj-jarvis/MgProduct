package fai.MgProductInfSvr.interfaces.utils;

import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;

import java.util.Calendar;

public class MgProductArg {

    private int aid;
    private int tid;
    private int siteId;
    private int lgId;
    private int keepPriId1;
    private FaiList<Param> primaryKeys;

    private FaiList<Param> addList;
    private boolean isBiz;
    private int rlPdId;
    private Calendar optTime;
    private long costPrice;
    private FaiList<Param> skuList;
    private FaiList<ParamUpdater> updaterList;

    private MgProductArg(Builder builder) {
        this.aid = builder.aid;
        this.tid = builder.tid;
        this.siteId = builder.siteId;
        this.lgId = builder.lgId;
        this.keepPriId1 = builder.keepPriId1;

        this.primaryKeys = builder.primaryKeys;
        this.updaterList = builder.updaterList;
        this.addList = builder.addList;
        this.isBiz = builder.isBiz;
        this.rlPdId = builder.rlPdId;
        this.optTime = builder.optTime;
        this.costPrice = builder.costPrice;
        this.skuList = builder.skuList;
    }

    public int getAid() {
        return aid;
    }

    public int getTid() {
        return tid;
    }

    public int getSiteId() {
        return siteId;
    }

    public int getLgId() {
        return lgId;
    }

    public int getKeepPriId1() {
        return keepPriId1;
    }

    public FaiList<Param> getPrimaryKeys() {
        return primaryKeys;
    }

    public FaiList<ParamUpdater> getUpdaterList() {
        return updaterList;
    }

    public FaiList<Param> getAddList() {
        return addList;
    }

    public FaiList<Param> getSkuList() {
        return skuList;
    }

    public boolean getIsBiz() {
        return isBiz;
    }

    public int getRlPdId() {
        return rlPdId;
    }

    public Calendar getOptTime() {
        return optTime;
    }

    public long getCostPrice() {
        return costPrice;
    }

    private static class TopBuilder {
        protected int aid;
        protected int tid;
        protected int siteId;
        protected int lgId;
        protected int keepPriId1;

        protected FaiList<Param> addList;
        protected FaiList<Param> primaryKeys;
        protected FaiList<Param> addList;
        protected FaiList<ParamUpdater> updaterList;
    }

    private static class BasicBuilder extends TopBuilder {
        protected int rlPdId;
    }

    private static class GroupBuilder extends BasicBuilder {
    }

    private static class PropBuilder extends GroupBuilder {
    }

    private static class SpecBuilder extends PropBuilder {
        protected FaiList<Param> skuList;
    }

    private static class StoreBuilder extends SpecBuilder {
        protected Calendar optTime;
        protected boolean isBiz;
        protected long costPrice;
    }

    public static class Builder extends StoreBuilder {
        public Builder (int aid, int tid, int siteId, int lgId, int keepPriId1) {
            this.aid = aid;
            this.tid = tid;
            this.siteId = siteId;
            this.lgId = lgId;
            this.keepPriId1 = keepPriId1;
        }

        public Builder setPrimaryList(FaiList<Param> primaryKeys) {
            if(primaryKeys == null || primaryKeys.isEmpty()) {
                throw new RuntimeException();
            }
            this.primaryKeys = primaryKeys;
            return this;
        }

        public Builder setUpdaterList(FaiList<ParamUpdater> updaterList) {
            if(updaterList == null || updaterList.isEmpty()) {
                throw new RuntimeException();
            }
            this.updaterList = updaterList;
            return this;
        }

        public Builder setSkuList(FaiList<Param> skuList) {
            if(skuList == null || skuList.isEmpty()) {
                throw new RuntimeException();
            }
            this.skuList = skuList;
            return this;
        }

        public Builder setAddList(FaiList<Param> addList) {
            if(addList == null || addList.isEmpty()) {
                throw new RuntimeException();
            }
            this.addList = addList;
            return this;
        }

        public Builder setIsBiz(boolean isBiz) {
            this.isBiz = isBiz;
            return this;
        }

        public Builder setRlPdId(int rlPdId) {
            this.rlPdId = rlPdId;
            return this;
        }

        public Builder setCostPrice(long costPrice) {
            this.costPrice = costPrice;
            return this;
        }

        public Builder setOptTime(Calendar optTime) {
            this.optTime = optTime;
            return this;
        }

        public MgProductArg build() {
            return new MgProductArg(this);
        }
    }
}
