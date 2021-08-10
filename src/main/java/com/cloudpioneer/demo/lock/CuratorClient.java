package com.cloudpioneer.demo.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * <类的详细说明：>
 *
 * @Author: Huanghai
 * @Version: 5/6/20
 **/
public class CuratorClient {


    public CuratorFramework getClient() {
        String hostPort = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
        return CuratorFrameworkFactory.newClient(hostPort,
                new ExponentialBackoffRetry(1000, 5));
    }
}
