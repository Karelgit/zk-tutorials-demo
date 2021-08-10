package com.cloudpioneer.demo;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * <类的详细说明：>
 *
 * @Author: Huanghai
 * @Version: 4/10/20
 **/
public class Client implements Watcher, Closeable {

    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toHexString(new Random().nextInt());

    public boolean isConnected() {
        return connected;
    }

    public boolean isExpired() {
        return expired;
    }

    volatile boolean connected = false;
    volatile boolean expired = false;

    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 60000, this);
    }

    void submitTask(String task, TaskObject taskCtx)  {
        taskCtx.setTask(task);
        zk.create("/tasks/task-",
                task.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallback,
                taskCtx);
    }

    AsyncCallback.StringCallback createTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc))   {
                case CONNECTIONLOSS:
                    submitTask(((TaskObject)ctx).getTask(),
                            (TaskObject)ctx);
                    break;
                case OK:
                    System.out.println("My created task name: " + name);
                    ((TaskObject) ctx).setTaskName(name);
                    watchStatus("/status" + name.replace("/task/",""),
                            ctx);
                    break;
                default:
                    System.out.println("Something went wrong" + KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };

    ConcurrentHashMap<String,Object> ctxMap = new ConcurrentHashMap<String, Object>();

    void watchStatus(String path ,Object ctx)   {
        ctxMap.put(path,ctx);
        zk.exists(path,statusWatcher,
                existsCallback,
                ctx);
    }

    Watcher statusWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeCreated)  {
                assert e.getPath().contains("/status/task-");

                zk.getData(e.getPath(),
                        false,
                        getDataCallback,
                        ctxMap.get(e.getPath()));
            }
        }
    };

    AsyncCallback.DataCallback getDataCallback = new AsyncCallback.DataCallback(){
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Try again.
                     */
                    zk.getData(path, false, getDataCallback, ctxMap.get(path));
                    return;
                case OK:
                    /*
                     *  Print result
                     */
                    String taskResult = new String(data);
                    System.out.println("Task " + path + ", " + taskResult);

                    /*
                     *  Setting the status of the task
                     */
                    assert(ctx != null);
                    ((TaskObject) ctx).setStatus(taskResult.contains("done"));

                    /*
                     *  Delete status znode
                     */
                    //zk.delete("/tasks/" + path.replace("/status/", ""), -1, taskDeleteCallback, null);
                    zk.delete(path, -1, taskDeleteCallback, null);
                    ctxMap.remove(path);
                    break;
                case NONODE:
//                    LOG.warn("Status node is gone!");
                    System.out.println("Status node is gone!");
                    return;
                default:
                   /* LOG.error("Something went wrong here, " +
                            KeeperException.create(Code.get(rc), path));*/
                    System.out.println("Something went wrong here, " +
                            KeeperException.create(KeeperException.Code.get(rc), path));

            }
        }
    };

    AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback(){
        @Override
        public void processResult(int rc, String path, Object ctx){
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.delete(path, -1, taskDeleteCallback, null);
                    break;
                case OK:
//                    LOG.info("Successfully deleted " + path);
                    System.out.println("Successfully deleted " + path);
                    break;
                default:
                    /*LOG.error("Something went wrong here, " +
                            KeeperException.create(Code.get(rc), path));*/
                    System.out.println("Something went wrong here, " +
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.StatCallback existsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(KeeperException.Code.get(rc))    {
                case CONNECTIONLOSS:
                    watchStatus(path,ctx);
                    break;
                case OK:
                    if(stat !=null) {
                        zk.getData(path,false,getDataCallback,null);

                    }
                    break;
                case NONODE:
                    break;
                default:
                    System.out.println("Something went wrong when " + "checking if the status node exists: "+
                            KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };

    static class  TaskObject {
        private String task;
        private String taskName;
        private boolean done = false;
        private boolean succesful = false;
        private CountDownLatch latch = new CountDownLatch(1);

        String getTask () {
            return task;
        }

        void setTask (String task) {
            this.task = task;
        }

        void setTaskName(String name){
            this.taskName = name;
        }

        String getTaskName (){
            return taskName;
        }

        void setStatus (boolean status){
            succesful = status;
            done = true;
            latch.countDown();
        }

        void waitUntilDone () {
            try{
                latch.await();
            } catch (InterruptedException e) {
                System.out.println("InterruptedException while waiting for task to get done");
            }
        }

        synchronized boolean isDone(){
            return done;
        }

        synchronized boolean isSuccesful(){
            return succesful;
        }

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void process(WatchedEvent e) {
        System.out.println(e);
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    System.out.println("Exiting due to session expiration");
                default:
                    break;
            }
        }
    }

    public static void main(String args[]) throws Exception {
        Client c = new Client("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        c.startZK();

        while(!c.isConnected()){
            Thread.sleep(100);
        }

        TaskObject task1 = new TaskObject();
        TaskObject task2 = new TaskObject();

        c.submitTask("Sample task", task1);
//        c.submitTask("Another sample task", task2);

        task1.waitUntilDone();
        task2.waitUntilDone();
    }
}
