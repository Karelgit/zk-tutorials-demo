package com.cloudpioneer.demo.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * <类的详细说明：>
 *
 * @Author: Huanghai
 * @Version: 5/7/20
 **/
public class LockDemo {

    private static String CONNECTION_STR = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static void main(String[] args) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().
                connectString(CONNECTION_STR).sessionTimeoutMs(5000).
                retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        curatorFramework.start();


//        final InterProcessMutex lock = new InterProcessMutex(curatorFramework, "/locks");
//        final InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(curatorFramework, "/locks");
        final InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(curatorFramework,"/locks");

        for (int i = 0; i < 10; i++) {
            final int j = i;
            new Thread(() -> {
                System.out.println(Thread.currentThread().getName() + "->尝试竞争读");
                try {
                    //阻塞竞争锁
                    lock.acquire();
                    System.out.println(Thread.currentThread().getName() + "->成功获得了锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        //释放锁
                        lock.release();
                        System.out.println(Thread.currentThread().getName() + "->成功释放了锁");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, "Thread-" + i).start();
        }
    }
}
