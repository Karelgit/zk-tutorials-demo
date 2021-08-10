package com.cloudpioneer.demo.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class CuratorExample {
    public static final String HOST_PORT = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public CuratorFramework getClient() {
        return CuratorFrameworkFactory.newClient(HOST_PORT,
                new ExponentialBackoffRetry(1000, 5));
    }

    public void createZnode()    throws Exception{
        CuratorFramework zkc = CuratorFrameworkFactory.newClient(HOST_PORT,
                new ExponentialBackoffRetry(1000, 5));
        zkc.create().withMode(CreateMode.PERSISTENT).inBackground().forPath("/mypath",new byte[0]);
    }
}
