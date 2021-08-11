package fai.MgProductSearchSvr.domain.comm;

import fai.comm.util.Log;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * @author LuChaoJi
 * @date 2021-08-04 15:43
 */
public class CliFactory {

    /**
     * 通过反射，完成cli的初始化，获取cli的实例
     * @param cliType cli的类型
     */
    public  static  <T> T getCliInstance(int flow, Class<T> cliType) {
        T cli = null;
        try {
            Constructor<T> constructor = cliType.getDeclaredConstructor(int.class);
            cli = constructor.newInstance(flow);
            Method initMethod = cliType.getDeclaredMethod("init");
            if (!(boolean) initMethod.invoke(cli)) {
                Log.logErr("init " + cliType.getSimpleName() + " err, flow=%s;", flow);
                cli = null;
            }
        } catch (Exception e) {
            Log.logErr(e, "create cli error;flow=%d;", flow);
        }
        return cli;
    }
}
