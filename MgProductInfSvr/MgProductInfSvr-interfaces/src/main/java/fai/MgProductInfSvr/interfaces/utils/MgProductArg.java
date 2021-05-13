package fai.MgProductInfSvr.interfaces.utils;

import fai.comm.util.FaiList;
import fai.comm.util.Param;

public class MgProductArg {

    private int aid;
    private int tid;
    private int siteId;
    private int lgId;
    private int keepPriId1;

    private FaiList<Param> addList;
    private boolean isBiz;

    private MgProductArg(Builder builder) {
        this.aid = builder.aid;
        this.tid = builder.tid;
        this.siteId = builder.siteId;
        this.lgId = builder.lgId;
        this.keepPriId1 = builder.keepPriId1;

        this.addList = builder.addList;
        this.isBiz = builder.isBiz;
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

    public FaiList<Param> getAddList() {
        return addList;
    }

    public boolean getIsBiz() {
        return isBiz;
    }

    public static class Builder {
        private int aid;
        private int tid;
        private int siteId;
        private int lgId;
        private int keepPriId1;

        private FaiList<Param> addList;
        private boolean isBiz;

        public Builder (int aid, int tid, int siteId, int lgId, int keepPriId1) {
            this.aid = aid;
            this.tid = tid;
            this.siteId = siteId;
            this.lgId = lgId;
            this.keepPriId1 = keepPriId1;
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

        public MgProductArg build() {
            return new MgProductArg(this);
        }
    }
}
