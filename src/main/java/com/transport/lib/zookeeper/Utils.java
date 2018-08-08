package com.transport.lib.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.zeromq.ZMQ;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;

@SuppressWarnings("WeakerAccess, unused")
public class Utils {

    public static ZooKeeper zk;

    public static ZooKeeperConnection conn;

    public static final ArrayList<String> services = new ArrayList<>();

    public static void connect(String url){
        try {
            conn = new ZooKeeperConnection();
            zk = conn.connect(url);
            ShutdownHook shutdownHook = new ShutdownHook();
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String getHostForService(String service, String moduleId){
        service = service.replace("Transport", "");
        try{
            Stat stat = znode_exists("/" +service);
            if(stat != null) {
                return getHostsForService("/" + service, moduleId)[0];
            }else throw new RuntimeException("No route for service: " + service);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    private static String[] getHostsForService(String service, String moduleId) throws KeeperException, InterruptedException, ParseException{
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray)new JSONParser().parse(new String(zkData));
        if(jArray.size() == 0)
            throw new RuntimeException("No route for service: " + service);
        else {
            ArrayList<String> hosts = new ArrayList<>();
            if(moduleId != null){
                for(int i = 0; i < jArray.size(); i++){
                    String host = (String)jArray.get(i);
                    if(host.endsWith("#" + moduleId))
                        hosts.add(host.replace("#" + moduleId, ""));
                }
                if(hosts.isEmpty())
                    throw new RuntimeException("No route for service: " + service + " and module.id " + moduleId);
            }else{
                for(int i = 0; i < jArray.size(); i++){
                    String host = (String)jArray.get(i);
                    hosts.add(host.substring(0, host.indexOf("#")));
                }
            }
            return hosts.toArray(new String[hosts.size()]);
        }
    }

    public static void registerService(String service){
        try {
            Stat stat = znode_exists("/" + service);
            if(stat != null) {
                update("/" + service);
            } else {
                create("/" + service);
            }
            services.add("/" + service);
            System.out.println("Registered service: " + service);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean useKafkaForAsync(){
        return Boolean.valueOf(System.getProperty("use.kafka.for.async", "false"));
    }

    public static boolean useKafkaForSync(){
        return Boolean.valueOf(System.getProperty("use.kafka.for.sync", "false"));
    }

    private static int getServicePort(){
        try {
            return Integer.parseInt(System.getProperty("service.port", "4242"));
        }catch (NumberFormatException e){
            return 4242;
        }
    }

    private static int getCallbackPort(){
        try {
            return Integer.parseInt(System.getProperty("service.port", "4242")) + 100;
        }catch (NumberFormatException e){
            return 4342;
        }
    }

    private static void create(String service) throws KeeperException,InterruptedException, UnknownHostException {
        JSONArray ja = new JSONArray();
        ja.add(getServiceBindAddress());
        zk.create(service, ja.toJSONString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    private static Stat znode_exists(String service) throws KeeperException,InterruptedException {
        return zk.exists(service, true);
    }

    private static void update(String service) throws  KeeperException,InterruptedException,ParseException, UnknownHostException {
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray)new JSONParser().parse(new String(zkData));
        String local = getServiceBindAddress();
        if(!jArray.contains(local)){
            jArray.add(local);
            zk.setData(service, jArray.toJSONString().getBytes(), zk.exists(service,true).getVersion());
        }
    }

    public static void delete(String service) throws  KeeperException,InterruptedException,ParseException, UnknownHostException {
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray)new JSONParser().parse(new String(zkData));
        String local = getServiceBindAddress();
        if(jArray.contains(local)){
            jArray.remove(local);
            zk.setData(service, jArray.toJSONString().getBytes(), zk.exists(service,true).getVersion());
        }
    }

    public static String getServiceBindAddress() throws UnknownHostException{
        return getLocalHostLANAddress().getHostAddress() + ":" + getServicePort() + "#" + System.getProperty("module.id");
    }

    public static String getZeroMQBindAddress() throws UnknownHostException{
        return getLocalHostLANAddress().getHostAddress() + ":" + getServicePort();
    }

    public static String getZeroMQCallbackBindAddress() throws UnknownHostException{
        return getLocalHostLANAddress().getHostAddress() + ":" + getCallbackPort();
    }

    private static InetAddress getLocalHostLANAddress() throws UnknownHostException {
        try {
            InetAddress candidateAddress = null;
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {

                        if (inetAddr.isSiteLocalAddress()) {
                            return inetAddr;
                        }
                        else if (candidateAddress == null) {
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
        }
        catch (Exception e) {
            UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
            unknownHostException.initCause(e);
            throw unknownHostException;
        }
    }

    public static void closeSocketAndContext(ZMQ.Socket socket, ZMQ.Context context){
        socket.close();
        if(!context.isClosed()){
            context.close();
            if(!context.isTerminated())
                context.term();
        }
    }
}

class ShutdownHook extends Thread {

    public void run() {
        try{
            for(String service : Utils.services){
                Utils.delete(service);
                System.out.println("Unregistered service: " + service);
            }
            Utils.conn.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
