package de.manmon.IPFIX.test;

import com.waltznetworks.jflowlib.jnetflow.header.MessageHeader;
import com.waltznetworks.jflowlib.util.HeaderParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class SocketTest
{
    
    public static void main(String[] args)
    {
        DatagramSocket ds = null;
        try {
            ds = new DatagramSocket(12345);
            while (true) {
                byte[] data = new byte[65536];
                DatagramPacket dp = new DatagramPacket(data, data.length);
                ds.receive(dp);
                System.out.print("dp.getData(): ");
                System.out.println(new String(dp.getData(), StandardCharsets.UTF_8));
                MessageHeader mh = MessageHeader.parse(dp.getData());
                System.out.println(mh);
            }
        } catch (SocketException se) {
            se.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (HeaderParseException hpe) {
            hpe.printStackTrace();
        } finally {
            if (ds != null) ds.close();
        }
    }
}
