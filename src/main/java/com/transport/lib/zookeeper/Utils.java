package com.transport.lib.zookeeper;

import com.transport.lib.common.Protocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;

@SuppressWarnings("all")
public class Utils {

    public static final ArrayList<String> services = new ArrayList<>();
    public static ZooKeeper zk;

    public static ZooKeeperConnection conn;
    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static void connect(String url) {
        try {
            conn = new ZooKeeperConnection();
            zk = conn.connect(url);
            ShutdownHook shutdownHook = new ShutdownHook();
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (Exception e) {
            logger.error("Can not connect to ZooKeeper cluster", e);
        }
    }

    public static String getHostForService(String service, String moduleId, Protocol protocol) {
        service = service.replace("Transport", "");
        Stat stat = null;
        try {
            stat = znode_exists("/" + service);
        } catch (Exception e) {
            logger.error("Can not connect to ZooKeeper cluster", e);
        }
        if (stat != null) {
            try {
                return getHostsForService("/" + service, moduleId, protocol)[0];
            } catch (Exception e) {
                throw new RuntimeException("No route for service: " + service);
            }
        } else throw new RuntimeException("No route for service: " + service);
    }

    private static String[] getHostsForService(String service, String moduleId, Protocol protocol) throws Exception {
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray) new JSONParser().parse(new String(zkData));
        if (jArray.size() == 0)
            throw new RuntimeException("No route for service: " + service);
        else {
            ArrayList<String> hosts = new ArrayList<>();
            for (int i = 0; i < jArray.size(); i++) {
                String registry = (String) jArray.get(i);
                String[] params = registry.split("#");
                if (moduleId != null) {
                    if (moduleId.equals(params[1]) && protocol.getRegName().equals(params[2])) hosts.add(params[0]);
                } else {
                    if (protocol.getRegName().equals(params[2])) hosts.add(params[0]);
                }
            }
            if (hosts.isEmpty())
                throw new RuntimeException("No route for service: " + service + " and module.id " + moduleId);
            return hosts.toArray(new String[hosts.size()]);
        }
    }

    public static String getModuleForService(String service, Protocol protocol) {
        try {
            byte[] zkData = zk.getData("/" + service, false, null);
            JSONArray jArray = (JSONArray) new JSONParser().parse(new String(zkData));
            if (jArray.size() == 0)
                throw new RuntimeException("No route for service: " + service);
            else {
                ArrayList<String> hosts = new ArrayList<>();
                for (int i = 0; i < jArray.size(); i++) {
                    String registry = (String) jArray.get(i);
                    String[] params = registry.split("#");
                    if (protocol.getRegName().equals(params[2])) hosts.add(params[1]);
                }
                if (hosts.isEmpty())
                    throw new RuntimeException("No route for service: " + service + " and protocol " + protocol.getRegName());
                return hosts.get(0);
            }
        } catch (Exception e) {
            logger.error("Error while getting avaiable module.id:", e);
            throw new RuntimeException("No route for service: " + service + " and protocol " + protocol.getRegName());
        }
    }

    public static void registerService(String service, Protocol protocol) {
        try {
            Stat stat = znode_exists("/" + service);
            if (stat != null) {
                update("/" + service, protocol);
            } else {
                create("/" + service, protocol);
            }
            services.add("/" + service);
            logger.info("Registered service: " + service);
        } catch (Exception e) {
            logger.error("Can not register services in ZooKeeper", e);
        }
    }

    public static boolean useKafka() {
        return Boolean.valueOf(System.getProperty("use.kafka", "false"));
    }

    private static int getServicePort() {
        try {
            return Integer.parseInt(System.getProperty("service.port", "4242"));
        } catch (NumberFormatException e) {
            return 4242;
        }
    }

    private static int getCallbackPort() {
        try {
            return Integer.parseInt(System.getProperty("service.port", "4242")) + 100;
        } catch (NumberFormatException e) {
            return 4342;
        }
    }

    private static void create(String service, Protocol protocol) throws KeeperException, InterruptedException, UnknownHostException {
        JSONArray ja = new JSONArray();
        ja.add(getServiceBindAddress(protocol));
        zk.create(service, ja.toJSONString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private static Stat znode_exists(String service) throws KeeperException, InterruptedException {
        return zk.exists(service, true);
    }

    private static void update(String service, Protocol protocol) throws KeeperException, InterruptedException, ParseException, UnknownHostException {
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray) new JSONParser().parse(new String(zkData));
        String local = getServiceBindAddress(protocol);
        if (!jArray.contains(local)) {
            jArray.add(local);
            zk.setData(service, jArray.toJSONString().getBytes(), zk.exists(service, true).getVersion());
        }
    }

    public static void delete(String service, Protocol protocol) throws KeeperException, InterruptedException, ParseException, UnknownHostException {
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray) new JSONParser().parse(new String(zkData));
        String local = getServiceBindAddress(protocol);
        if (jArray.contains(local)) {
            jArray.remove(local);
            zk.setData(service, jArray.toJSONString().getBytes(), zk.exists(service, true).getVersion());
        }
    }

    public static String getServiceBindAddress(Protocol protocol) throws UnknownHostException {
        return getLocalHostLANAddress().getHostAddress() + ":" + getServicePort() + "#" + System.getProperty("module.id") + "#" + protocol.getRegName();
    }

    public static String getZeroMQBindAddress() throws UnknownHostException {
        return getLocalHostLANAddress().getHostAddress() + ":" + getServicePort();
    }

    public static String getZeroMQCallbackBindAddress() throws UnknownHostException {
        return getLocalHostLANAddress().getHostAddress() + ":" + getCallbackPort();
    }

    private static InetAddress getLocalHostLANAddress() throws UnknownHostException {
        try {
            InetAddress candidateAddress = null;
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {

                        if (inetAddr.isSiteLocalAddress()) {
                            return inetAddr;
                        } else if (candidateAddress == null) {
                            candidateAddress = inetAddr;
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                return candidateAddress;
            }
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            if (jdkSuppliedAddress == null) {
                throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
            }
            return jdkSuppliedAddress;
        } catch (Exception e) {
            UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
            unknownHostException.initCause(e);
            throw unknownHostException;
        }
    }

    public static void closeSocketAndContext(ZMQ.Socket socket, ZMQ.Context context) {
        socket.close();
        if (!context.isClosed()) {
            context.close();
            if (!context.isTerminated())
                context.term();
        }
    }
}

class ShutdownHook extends Thread {
    public void run() {
        try {
            for (String service : Utils.services) {
                Utils.delete(service, Protocol.KAFKA);
                Utils.delete(service, Protocol.ZMQ);
            }
            Utils.conn.close();
        } catch (Exception ignore) {
        }
    }
}
