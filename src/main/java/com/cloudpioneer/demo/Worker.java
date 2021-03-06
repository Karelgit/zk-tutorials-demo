package com.cloudpioneer.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <类的详细说明：>
 *
 * @Author: Huanghai
 * @Version: 4/9/20
 **/
public class Worker implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString((new Random()).nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    /*
     * In general, it is not a good idea to block the callback thread
     * of the ZooKeeper client. We use a thread pool executor to detach
     * the computation from the callback.
     */
    private ThreadPoolExecutor executor;

    /**
     * Creates a new Worker instance.
     *
     * @param hostPort
     */
    public Worker(String hostPort) {
        this.hostPort = hostPort;
        this.executor = new ThreadPoolExecutor(1, 1,
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(200));
    }

    /**
     * Creates a ZooKeeper session.
     *
     * @throws IOException
     */
    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    /**
     * Deals with session events like connecting
     * and disconnecting.
     *
     * @param e new event generated
     */
    @Override
    public void process(WatchedEvent e) {
        LOG.info(e.toString() + ", " + hostPort);
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
                case SyncConnected:
                    /*
                     * Registered with ZooKeeper
                     */
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expired");
                default:
                    break;
            }
        }
    }

    /**
     * Checks if this client is connected.
     *
     * @return boolean
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * Checks if ZooKeeper session is expired.
     *
     * @return
     */
    public boolean isExpired() {
        return expired;
    }

    /**
     * Bootstrapping here is just creating a /assign parent
     * znode to hold the tasks assigned to this worker.
     */
    public void bootstrap(){
        createAssignNode();
    }

    void createAssignNode(){
        zk.create("/assign/worker-" + serverId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createAssignCallback, null);
    }

    AsyncCallback.StringCallback createAssignCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Try again. Note that registering again is not a problem.
                     * If the znode has already been created, then we get a
                     * NODEEXISTS event back.
                     */
                    createAssignNode();
                    break;
                case OK:
                    LOG.info("Assign node created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Assign node already registered");
                    break;
                default:
                    LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    String name;

    /**
     * Registering the new worker, which consists of adding a worker
     * znode to /workers.
     */
    public void register(){
        name = "worker-" + serverId;
        zk.create("/workers/" + name,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Try again. Note that registering again is not a problem.
                     * If the znode has already been created, then we get a
                     * NODEEXISTS event back.
                     */
                    register();

                    break;
                case OK:
                    LOG.info("Registered successfully: " + serverId);

                    break;
                case NODEEXISTS:
                    LOG.warn("Already registered: " + serverId);

                    break;
                default:
                    LOG.error("Something went wrong: ",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String)ctx);
                    return;
            }
        }
    };

    String status;
    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            zk.setData("/workers/" + name, status.getBytes(), -1,
                    statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    private int executionCount;

    synchronized void changeExecutionCount(int countChange) {
        executionCount += countChange;
        if (executionCount == 0 && countChange < 0) {
            // we have just become idle
            setStatus("Idle");
        }
        if (executionCount == 1 && countChange > 0) {
            // we have just become idle
            setStatus("Working");
        }
    }
    /*
     ***************************************
     ***************************************
     * Methods to wait for new assignments.*
     ***************************************
     ***************************************
     */

    Watcher newTaskWatcher = new Watcher(){
        @Override
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeChildrenChanged) {
                assert new String("/assign/worker-"+ serverId ).equals( e.getPath() );

                getTasks();
            }
        }
    };

    void getTasks(){
        zk.getChildren("/assign/worker-" + serverId,
                newTaskWatcher,
                tasksGetChildrenCallback,
                null);
    }


    protected ChildrenCache assignedTasksCache = new ChildrenCache();

    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if(children != null){
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback cb;

                            /*
                             * Initializes input of anonymous class
                             */
                            public Runnable init (List<String> children, DataCallback cb) {
                                this.children = children;
                                this.cb = cb;

                                return this;
                            }

                            @Override
                            public void run() {
                                if(children == null) {
                                    return;
                                }

                                LOG.info("Looping into tasks");
                                setStatus("Working");
                                for(String task : children){
                                    LOG.trace("New task: {}", task);
                                    zk.getData("/assign/worker-" + serverId  + "/" + task,
                                            false,
                                            cb,
                                            task);
                                }
                            }
                        }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                    }
                    break;
                default:
                    System.out.println("getChildren failed: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.getData(path, false, taskDataCallback, null);
                    break;
                case OK:
                    /*
                     *  Executing a task in this example is simply printing out
                     *  some string representing the task.
                     */
                    executor.execute( new Runnable() {
                        byte[] data;
                        Object ctx;

                        /*
                         * Initializes the variables this anonymous class needs
                         */
                        public Runnable init(byte[] data, Object ctx) {
                            this.data = data;
                            this.ctx = ctx;

                            return this;
                        }

                        @Override
                        public void run() {
                            LOG.info("Executing your task: " + new String(data));
                            zk.create("/status/" + (String) ctx, "done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT, taskStatusCreateCallback, null);
                            zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
                                    -1, taskVoidCallback, null);
                        }
                    }.init(data, ctx));

                    break;
                default:
                    LOG.error("Failed to get task data: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.StringCallback taskStatusCreateCallback = new AsyncCallback.StringCallback(){
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.create(path + "/status", "done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                            taskStatusCreateCallback, null);
                    break;
                case OK:
                    LOG.info("Created status znode correctly: " + name);
                    break;
                case NODEEXISTS:
                    LOG.warn("Node exists: " + path);
                    break;
                default:
                    LOG.error("Failed to create task data: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }

        }
    };

    AsyncCallback.VoidCallback taskVoidCallback = new AsyncCallback.VoidCallback(){
        @Override
        public void processResult(int rc, String path, Object rtx){
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    break;
                case OK:
                    LOG.info("Task correctly deleted: " + path);
                    break;
                default:
                    LOG.error("Failed to delete task data" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * Closes the ZooKeeper session.
     */
    @Override
    public void close()
            throws IOException
    {
        LOG.info( "Closing" );
        try{
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
    }

    /**
     * Main method showing the steps to execute a worker.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        Worker w = new Worker("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        w.startZK();

        while(!w.isConnected()){
            Thread.sleep(100);
        }
        /*
         * bootstrap() create some necessary znodes.
         */
        w.bootstrap();

        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w.register();

        /*
         * Getting assigned tasks.
         */
        w.getTasks();

        while(!w.isExpired()){
            Thread.sleep(1000);
        }

    }

}
