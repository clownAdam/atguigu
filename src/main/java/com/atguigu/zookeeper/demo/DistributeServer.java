package com.atguigu.zookeeper.demo;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author clown
 */
public class DistributeServer {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer server = new DistributeServer();
        //1.链接zookeeper集群
        server.getConnect();
        //2.注册server节点
        server.regist(args[0]);
        //3.业务逻辑
        server.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void regist(String hostname) throws KeeperException, InterruptedException {
        String path = zkClient.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+"--is online--");
    }

    private String connectString = "bd-101:2181,bd-102:2181,bd-103:2181,bd-104:2181,bd-105:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;
    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

}
