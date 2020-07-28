package flink.checkpoint.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.Properties;

/*
  *
  *@author sunmingqi
  *@date 2020/7/24
  */
public class PropertiesUtil {
    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);
    private static  Properties properties;

    static {
        String path= "src/main/prop/flink_kafka.properties";
        File file = new File(path);
        properties = new Properties();
        try {
            if (file .exists()&&file.isFile()) {
                properties.load(new FileReader(file));
            }else{
                try {
                    properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(path));

                } catch ( IOException e) {
                    e.printStackTrace();

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        }
/*
  *
  *
  *@params [key]
  *@return java.lang.String
  */
public static String getvalue(String key){
        String value = properties.getProperty(key);
        return value;
    }






}
