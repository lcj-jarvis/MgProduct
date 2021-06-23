package fai.MgProductLibSvr.interfaces.cli;

import fai.comm.netkit.FaiClient;
import fai.comm.util.Param;
import fai.comm.util.Str;
import fai.middleground.infutil.MgConfPool;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:15
 */
public class MgProductLibCli extends FaiClient {

    public MgProductLibCli(int flow, String name) {
        super(flow, name);
    }

    /**
     * <p> 初始化，开启配置中心的配置
     * @return
     */
    public boolean init() {
        return init("MgProductLibCli", true);
    }

    /**
     * 配置开启使用库服务
     * @return
     */
    public static boolean useProductLib() {
        Param mgSwitch = MgConfPool.getEnvConf("mgSwitch");
        if(Str.isEmpty(mgSwitch)) {
            return false;
        }
        boolean useProductGroup = mgSwitch.getBoolean("useProductLib", false);
        return useProductGroup;
    }
}
