# MgProductGroupSvr 注意点

## 分类名称的检查

**场景**：不同业务对分类名称的校验有所不同，所以中台对应不同业务也要做出适应



**例如**：门店在添加分类名称时候，是不允许名称重复的；而建站则允许重复；


**解决**：中台通过 tid 判断当前操作数据属于哪个业务，进而对校验做区分

先看大概的代码实现：

```java
// 根据 tid 获取业务名称
String businessName = BusinessMapping.getName(tid);
// 通过读取配置文件，判断是否进行分类名称的校验
boolean isCheck = isCheckGroupName(businessName);
if (isCheck) {
    // 从 db 查询 aid + uid 维度下的分类名称
    SearchArg searchArg = new SearchArg();
    searchArg.matcher = new ParamMatcher(ProductGroupEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.EQ, unionPriId);
    FaiList<Param> nameList = searchFromDb(aid, searchArg, ProductGroupEntity.Info.GROUP_NAME);
    String name = info.getString(ProductGroupEntity.Info.GROUP_NAME);
    Param existInfo = Misc.getFirst(nameList, ProductGroupEntity.Info.GROUP_NAME, name);
    if(!Str.isEmpty(existInfo)) {
        rt = Errno.ALREADY_EXISTED;
        throw new MgException(rt, "group name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, name);
    }
}
```

首先要区分 tid 属于哪个业务方，这个在 [中台公共包](http://gitlab.faidev.cc/middleground/fai-comm-middleground/blob/master/src/main/java/fai/comm/middleground/FaiValObj.java) 中已经做了定义，这边只是通过一个枚举类来映射，代码如下

```java
/**
 * 业务映射
 */
public enum  BusinessMapping {

    SITE("SITE", FaiValObj.TermId.SITE),
    HD("HD", FaiValObj.TermId.HD),
    CD("CD", FaiValObj.TermId.CD),
    TS("TS", FaiValObj.TermId.TS),
    YK("YK", FaiValObj.TermId.YK),
    MEDIA("MEDIA", FaiValObj.TermId.MEDIA),
    OPT("OPT", FaiValObj.TermId.OPT),
    KC("KC", FaiValObj.TermId.KC),
    MP("MP", FaiValObj.TermId.MP),
    MALL("MALL", FaiValObj.TermId.MALL),
    QZ("QZ", FaiValObj.TermId.QZ),
    EDU("EDU", FaiValObj.TermId.EDU),;

    /** 业务名称 */
    private String name;
    /** 业务对应的值 */
    private int tid;

    BusinessMapping(String name, int tid) {
        this.name = name;
        this.tid = tid;
    }

    public static String getName(int tid) {
        for (BusinessMapping bs : BusinessMapping.values()) {
            if (bs.tid == tid) {
                return bs.name;
            }
        }
        return null;
    }
}
```

其次是通过配置文件来控制校验的开关，可见 [配置中心](http://config.aaa.cn/?serviceTicket=st-24bf2386-5e09-4615-8253-fc82cc4b0f39) — MgPdCheckGroupNameSwitch 配置，代码如下

```java
/**
* 获取配置文件，是否检查分类名称重复
* @param name 业务名称 eg: YK , SITE
* @return boolean 是否检查
*/
private boolean isCheckGroupName(String name) {
    if (Str.isEmpty(name)) {
        throw new MgException(Errno.ERROR, "tid is illegal;flow=%d;", m_flow);
    }
    Param conf = MgConfPool.getEnvConf("MgPdCheckGroupNameSwitch");
    if (Str.isEmpty(conf)) {
        return false;
    }
    return conf.getBoolean(name, false);
}
```

最后根据开关，是否校验名称即可
