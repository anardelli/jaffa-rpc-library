package com.transport.lib.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;

public class ZKUtils {

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

    public static String getHostForService(String service){
        service = service.replace("Transport", "");
        try{
            Stat stat = znode_exists("/" +service);
            if(stat != null) {
                return getHostsForService("/" + service)[0];
            }else throw new RuntimeException("No route for service: " + service);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    private static String[] getHostsForService(String service) throws KeeperException, InterruptedException, ParseException{
        String[] hosts;
        byte[] zkData = zk.getData(service, false, null);
        JSONArray jArray = (JSONArray)new JSONParser().parse(new String(zkData));
        if(jArray.size() == 0)
            throw new RuntimeException("No route for service: " + service);
        else {
            hosts = new String[jArray.size()];
            for(int i = 0; i < jArray.size(); i++){
                hosts[i] = (String)jArray.get(i);
            }
            return hosts;
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

    private static int getServicePort(){
        try {
            return Integer.parseInt(System.getProperty("service.port", "4242"));
        }catch (NumberFormatException e){
            return 4242;
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
        return getLocalHostLANAddress().getHostAddress() + ":" + getServicePort();
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
}

class ShutdownHook extends Thread {

    public void run() {
        try{
            for(String service : ZKUtils.services){
                ZKUtils.delete(service);
                System.out.println("Unregistered service: " + service);
            }
            ZKUtils.conn.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
