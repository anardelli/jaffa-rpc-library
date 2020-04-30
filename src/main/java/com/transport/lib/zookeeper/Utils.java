package com.transport.lib.zookeeper;

import com.transport.lib.entities.Protocol;
import com.transport.lib.exception.TransportNoRouteException;
import com.transport.lib.exception.TransportSystemException;
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

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class Utils {

    public static final List<String> services = new ArrayList<>();
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);
    public static ZooKeeperConnection conn;
    private static ZooKeeper zk;

    /*
        Connects to ZooKeeper cluster
     */
    public static void connect(String url) {
        try {
            conn = new ZooKeeperConnection();
            zk = conn.connect(url);
            ShutdownHook shutdownHook = new ShutdownHook();
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (IOException | InterruptedException e) {
            logger.error("Can not connect to ZooKeeper cluster", e);
            throw new TransportSystemException(e);
        }
    }

    /*
        Returns first available host for specified service, module.id and protocol or throws TransportNoRouteException if no available
     */
    public static String getHostForService(String service, String moduleId, Protocol protocol) {
        service = service.replace("Transport", "");
        Stat stat = null;
        try {
            stat = isZNodeExists("/" + service);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Can not connect to ZooKeeper cluster", e);
            throw new TransportSystemException(e);
        }
        if (stat != null) {
            try {
                String host =  getHostsForService("/" + service, moduleId, protocol)[0];
                if(protocol.equals(Protocol.HTTP)){
                    host = getHttpPrefix() + host;
                }
                return host;
            } catch (KeeperException | ParseException | InterruptedException e) {
                throw new TransportNoRouteException(service);
            }
        } else throw new TransportNoRouteException(service);
    }

    private static String getHttpPrefix(){
        return (Boolean.parseBoolean(System.getProperty("http.ssl.enabled", "false")) ? "https" : "http") + "://";
    }

    /*
        Returns all available hosts for specified service, module.id and protocol or throws TransportNoRouteException if no available
     */
    private static String[] getHostsForService(String service, String moduleId, Protocol protocol) throws KeeperException, ParseException, InterruptedException {
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray) new JSONParser().parse(new String(zkData));
        if (jArray.isEmpty())
            throw new TransportNoRouteException(service);
        else {
            ArrayList<String> hosts = new ArrayList<>();
            for (Object json : jArray) {
                String[] params = ((String) json).split("#");
                if (moduleId != null) {
                    if (moduleId.equals(params[1]) && protocol.getShortName().equals(params[2])) hosts.add(params[0]);
                } else {
                    if (protocol.getShortName().equals(params[2])) hosts.add(params[0]);
                }
            }
            if (hosts.isEmpty())
                throw new TransportNoRouteException(service, moduleId);
            return hosts.toArray(new String[0]);
        }
    }

    /*
        Returns active module.id for specified service name and protocol or throws TransportNoRouteException if no available
     */
    public static String getModuleForService(String service, Protocol protocol) {
        try {
            byte[] zkData = zk.getData("/" + service, false, null);
            JSONArray jArray = (JSONArray) new JSONParser().parse(new String(zkData));
            if (jArray.isEmpty())
                throw new TransportNoRouteException(service);
            else {
                ArrayList<String> hosts = new ArrayList<>();
                for (Object json : jArray) {
                    String[] params = ((String) json).split("#");
                    if (protocol.getShortName().equals(params[2])) hosts.add(params[1]);
                }
                if (hosts.isEmpty())
                    throw new TransportNoRouteException(service, protocol);
                return hosts.get(0);
            }
        } catch (KeeperException | InterruptedException | ParseException e) {
            logger.error("Error while getting avaiable module.id:", e);
            throw new TransportNoRouteException(service, protocol.getShortName());
        }
    }

    /*
        Registers service with specified name and protocol in ZooKeeper cluster
     */
    public static void registerService(String service, Protocol protocol) {
        try {
            Stat stat = isZNodeExists("/" + service);
            if (stat != null) {
                update("/" + service, protocol);
            } else {
                create("/" + service, protocol);
            }
            services.add("/" + service);
            logger.info("Registered service: {}", service);
        } catch (KeeperException | InterruptedException | UnknownHostException | ParseException e) {
            logger.error("Can not register services in ZooKeeper", e);
            throw new TransportSystemException(e);
        }
    }

    /*
        Returns should we use Kafka or ZeroMQ
     */
    public static Protocol getTransportProtocol() {
        return Protocol.getByName(System.getProperty("transport.protocol"));
    }

    /*
        Returns user-provided service port or default if not
     */
    private static int getServicePort() {
        int defaultPort = 4242;
        try {
            return Integer.parseInt(System.getProperty(getTransportProtocol().getShortName() + ".service.port", String.valueOf(defaultPort)));
        } catch (NumberFormatException e) {
            return defaultPort;
        }
    }

    /*
        Returns user-provided callback port or default if not
     */
    private static int getCallbackPort() {
        int defaultPort = 4342;
        try {
            return Integer.parseInt(System.getProperty(getTransportProtocol().getShortName() + ".callback.port", String.valueOf(defaultPort)));
        } catch (NumberFormatException e) {
            return defaultPort;
        }
    }

    /*
        Creates service metadata information in ZooKeeper cluster
     */
    @SuppressWarnings("unchecked")
    private static void create(String service, Protocol protocol) throws KeeperException, InterruptedException, UnknownHostException {
        JSONArray ja = new JSONArray();
        ja.add(getServiceBindAddress(protocol));
        zk.create(service, ja.toJSONString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /*
        Check if znode with specified string exists in ZooKeeper cluster
     */
    private static Stat isZNodeExists(String service) throws KeeperException, InterruptedException {
        return zk.exists(service, true);
    }

    /*
        Updates service metadata information in ZooKeeper cluster
     */
    private static void update(String service, Protocol protocol) throws KeeperException, InterruptedException, ParseException, UnknownHostException {
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray) new JSONParser().parse(new String(zkData));
        String local = getServiceBindAddress(protocol);
        if (!jArray.contains(local)) {
            jArray.add(local);
            zk.setData(service, jArray.toJSONString().getBytes(), zk.exists(service, true).getVersion());
        }
    }

    /*
        Unpublishes service in ZooKeeper cluster
     */
    public static void delete(String service, Protocol protocol) throws KeeperException, InterruptedException, ParseException, UnknownHostException {
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray) new JSONParser().parse(new String(zkData));
        String local = getServiceBindAddress(protocol);
        if (jArray.contains(local)) {
            jArray.remove(local);
            zk.setData(service, jArray.toJSONString().getBytes(), zk.exists(service, true).getVersion());
        }
    }

    /*
        Returns string for registration/publishing service in ZooKeeper cluster
     */
    private static String getServiceBindAddress(Protocol protocol) throws UnknownHostException {
        return getLocalHostLANAddress().getHostAddress() + ":" + getServicePort() + "#" + System.getProperty("module.id") + "#" + protocol.getShortName();
    }

    /*
        Returns ZeroMQ connection string for receiving sync and async requests from client
     */
    public static String getZeroMQBindAddress() throws UnknownHostException {
        return getLocalHostLANAddress().getHostAddress() + ":" + getServicePort();
    }

    public static InetSocketAddress getHttpBindAddress() throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getLocalHost(),  getServicePort());
    }

    /*
        Returns ZeroMQ connection string for receiving async responses from server
     */
    public static String getZeroMQCallbackBindAddress() throws UnknownHostException {
        return getLocalHostLANAddress().getHostAddress() + ":" + getCallbackPort();
    }

    /*
        Returns HTTP connection string for receiving async responses from server
     */
    public static InetSocketAddress getHttpCallbackBindAddress() throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getLocalHost(), getCallbackPort());
    }

    /*
        Returns HTTP connection string for receiving async responses from server
     */
    public static String getHttpCallbackStringAddress() throws UnknownHostException {
        return getHttpPrefix() + getLocalHostLANAddress().getHostAddress() + ":" + getCallbackPort();
    }

    /*
        Returns local hostname
     */
    private static InetAddress getLocalHostLANAddress() throws UnknownHostException {
        try {
            InetAddress candidateAddress = null;
            for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
                NetworkInterface iface = ifaces.nextElement();
                for (Enumeration<InetAddress> inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = inetAddrs.nextElement();
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
        } catch (SocketException e) {
            UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
            unknownHostException.initCause(e);
            throw unknownHostException;
        }
    }

    /*
        Utility method for shutting down ZeroMQ socket and context
     */
    public static void closeSocketAndContext(ZMQ.Socket socket, ZMQ.Context context) {
        socket.close();
        if (!context.isClosed()) {
            context.close();
            if (!context.isTerminated())
                context.term();
        }
    }
}

/*
    We need to unpublish all services if JVM is shutting down
 */
class ShutdownHook extends Thread {
    @Override
    public void run() {
        try {
            for (String service : Utils.services) {
                Utils.delete(service, Protocol.KAFKA);
                Utils.delete(service, Protocol.ZMQ);
            }
            Utils.conn.close();
        } catch (KeeperException | InterruptedException | ParseException | IOException e) {
            throw new TransportSystemException(e);
        }
    }
}
