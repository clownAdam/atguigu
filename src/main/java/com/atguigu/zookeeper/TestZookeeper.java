package com.atguigu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import javax.sound.midi.Soundbank;
import java.io.IOException;
import java.util.List;

/**
 * @author Administrator
 */
public class TestZookeeper {
    private static String connectString = "bd-101:2181,bd-102:2181,bd-103:2181,bd-104:2181,bd-105:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;
    /**
     * 3.判断节点是否存在
     */
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/atguigu", false);
        System.out.println(stat==null?"exists":"no exists");
    }

    /**
     * 2.获取子节点病监控数据的变化
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 1.创建节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/atguigu", "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    /**
     * init
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //收到事件通知后的回调函数
                System.out.println(watchedEvent.getType() + "--->path:" + watchedEvent.getPath());
                //再次启动监听
                try {
                    zkClient.getChildren("/", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
