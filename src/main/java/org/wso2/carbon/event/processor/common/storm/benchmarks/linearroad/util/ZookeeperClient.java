package org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.util;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * Created by miyurud on 5/26/15.
 */
public class ZookeeperClient implements Watcher {
    public void create(String path){
        ZooKeeper zk = null;

        // if(zk == null){
        try {
            zk = new ZooKeeper(Config.getConfigurationInfo("org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.zookeeper.host"), 3000, this);

            zk.create(path, path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.close();
        } catch (IOException e) {
            System.out.println(e.toString());
            zk = null;
        } catch(InterruptedException intrEx) {
            // do something to prevent the program from crashing
            //System.out.println("\"new_znode exists!");
        } catch(KeeperException kpEx) {
            // do something to prevent the program from crashing
            //System.out.println("\"new_znode exists!");
        }
        // }
    }

    //zk.exists(path, true)

    public boolean exists(String path){
        boolean result = false;
        ZooKeeper zk = null;

        // if(zk == null){
        try {
            zk = new ZooKeeper(Config.getConfigurationInfo("org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.zookeeper.host"), 3000, this);

            byte[] res = zk.getData(path, true, zk.exists(path, true));

            result = true;
            zk.close();
        } catch (IOException e) {
            //System.out.println(e.toString());
            zk = null;
        } catch(InterruptedException intrEx) {
            // do something to prevent the program from crashing
            intrEx.printStackTrace();
        } catch(KeeperException.NoNodeException nn){
            result = false;
        } catch(KeeperException kpEx) {
            // do something to prevent the program from crashing
            kpEx.printStackTrace();
        }
        // }

        return result;
    }

    public boolean delete(String path){
        boolean result = false;
        ZooKeeper zk = null;

        //if(zk == null){
        try {
            zk = new ZooKeeper(Config.getConfigurationInfo("org.wso2.carbon.event.processor.common.storm.benchmarks.linearroad.zookeeper.host"), 3000, this);

            zk.delete(path, -1);
            result = true;
            zk.close();
        } catch (IOException e) {
            System.out.println(e.toString());
            zk = null;
        } catch(InterruptedException intrEx) {
            // do something to prevent the program from crashing
            System.out.println("AAAA");
        } catch(KeeperException.NoNodeException nn){

        }catch(KeeperException kpEx) {
            // do something to prevent the program from crashing
            System.out.println("BBBB");
            //System.out.println(kpEx.toString());
            kpEx.printStackTrace();
        }
        // }

        return result;
    }

    public void process(WatchedEvent watchedEvent) {

    }
}
