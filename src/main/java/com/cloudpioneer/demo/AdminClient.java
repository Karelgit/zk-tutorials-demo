package com.cloudpioneer.demo;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;

/**
 * <类的详细说明：>
 *
 * @Author: Huanghai
 * @Version: 4/9/20
 **/
public class AdminClient implements Watcher {

    ZooKeeper zk;
    String hostPort;

    AdminClient(String hostPort)   {
        this.hostPort = hostPort;
    }

    void start() throws Exception   {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void listState() throws KeeperException {
        try {
            Stat stat  = new Stat();
            byte masterData [] = zk.getData("/master",false,stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + "since " + startDate);
        }catch (KeeperException.NoNodeException e)  {
            System.out.println("No Master");
        }catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Workers:");
        try {
            for(String w: zk.getChildren("/workers",false)) {
                byte data[] =  zk.getData("/workers/" + w, false, null);
                String state = new String(data);
                System.out.println("\t" + w + ":" + state);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Tasks:");
        try {
            for (String t: zk.getChildren("/assign",false)) {
                System.out.println("\t" + t);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public static void main(String[] args) {
        AdminClient c = new AdminClient("127.0.0.1:2181");
        try {
            c.start();
            c.listState();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
