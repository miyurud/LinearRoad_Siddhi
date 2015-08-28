package org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.util;

/**
 * Created by miyurud on 6/26/15.
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    public static String getConfigurationInfo(String key){
        Properties props = new Properties();
        InputStream inStream = null;
        String value = null;

        try {
           // inStream = new FileInputStream("linearroad.properties");
            //props.load(inStream);
            props.load(Config.class.getClassLoader().getResourceAsStream("linearroad.properties"));
            value = props.getProperty(key);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return value;
    }
}
