package com.iecas.kds.tools.redis;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;



/**
 * Created by Administrator on 2016/3/29.
 */
public class Properties_redis {

    private static final Logger logger = LogManager
            .getLogger(Properties_redis.class);

    //application.properties配置文件属性对象
    public static Properties application = null;

    static {
        try {
            application = new Properties();

            //如果存在外部配置文件则优先加载外部配置文件
            File f = new File("redis.properties");
            if(f.exists()){
                //	System.out.println("加载外部配置文件："+f.getAbsolutePath());
                application.load(new FileReader(f));
            }else{
                //	System.out.println("加载jar包中配置文件。");
                application.load(Properties_redis.class.getResourceAsStream("/redis.properties"));
            }
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
