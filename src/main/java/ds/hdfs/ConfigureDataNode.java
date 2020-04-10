package ds.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

public class ConfigureDataNode {
    public static void main(String[] args){
        Properties props = new Properties();
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            String nodeName = inetAddress.getHostName();
            String nodeIp = inetAddress.getHostAddress();
            String port;

            File file = new File(nodeName + ".properties");
            FileOutputStream fileOutputStream = new FileOutputStream(file);

            if(args.length == 0){
                // If no port number is passed, default value of port is 1099
                port = "1099";
            }else{
                port = args[0];
            }

            props.setProperty("server_name", nodeName);
            props.setProperty("server_ip", nodeIp);
            props.setProperty("server_port", port);
            props.store(fileOutputStream, null);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
