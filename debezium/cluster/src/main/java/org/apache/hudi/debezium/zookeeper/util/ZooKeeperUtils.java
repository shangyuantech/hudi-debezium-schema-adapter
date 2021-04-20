package org.apache.hudi.debezium.zookeeper.util;

import org.apache.commons.lang.StringUtils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class ZooKeeperUtils {

    public static String getTopicsPath(String service) {
        return String.format("/%s/topics", service);
    }

    public static String getMaterPath(String service) {
        return String.format("/%s/master", service);
    }

    public static String getSlaveBasePath(String service) {
        return String.format("/%s/slave", service);
    }

    public static String getSlavePath(String service) {
        String hostName = getInnetIp();
        return String.format("%s/%s-%s", getSlaveBasePath(service), hostName, System.currentTimeMillis());
    }

    public static String getInnetIp() {
        String hostName = System.getenv("HOSTNAME");
        if (StringUtils.isNotBlank(hostName)) {
            return hostName;
        }

        String localip = null;
        String netip = null;

        try {
            Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            boolean finded = false;//  «∑Ò’“µΩÕ‚Õ¯IP
            while (netInterfaces.hasMoreElements() && !finded) {
                NetworkInterface ni = netInterfaces.nextElement();
                Enumeration<InetAddress> address = ni.getInetAddresses();
                while (address.hasMoreElements()) {
                    ip = address.nextElement();
                    if (!ip.isSiteLocalAddress()
                            && !ip.isLoopbackAddress()
                            && !ip.getHostAddress().contains(":")) {
                        netip = ip.getHostAddress();
                        finded = true;
                        break;
                    } else if (ip.isSiteLocalAddress()
                            && !ip.isLoopbackAddress()
                            && !ip.getHostAddress().contains(":")) {
                        localip = ip.getHostAddress();
                    }
                }
            }
        } catch (SocketException e1) {
            e1.printStackTrace();

            try {
                localip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e2) {
                e2.printStackTrace();
            }
        }

        if (netip != null && !"".equals(netip)) {
            return netip;
        } else {
            return localip;
        }
    }

}
