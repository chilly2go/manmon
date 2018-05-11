package de.manmon.test.japes.nasty.collector;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.Date;


public class Collector
{
    
    private              DatagramSocket sock;
    private static final int            MAX_UDP_SIZE = 65535;
    private              UdpBuffer      udpBuf       = new UdpBuffer();
    private              Connection     con          = null;
    private              Statement      s            = null;
    Timer     aggTimer;
    TimerTask aggTask;
    
    private static FlowsetPacket netflowPack = null;
    private static FlowsetPacket ipfixPack   = null;
    
    public Collector()
    {
        
        try
        {
            sock = new DatagramSocket(12345);
        } catch (SocketException e)
        {
            System.out.println("Can't open socket on port 2055.");
            System.exit(-1);
        }
        
        openDatabase();
        
        if (con == null || s == null)
        {
            
            System.err.println("Database connection not established.");
            System.exit(-1);
        }
        /*
        try
        {
            s.executeUpdate("DELETE FROM config");
            s.executeUpdate("INSERT INTO config VALUES(" + ConfigParser.getDaysDetailedData() + "," +
                            ConfigParser.getWeeksDailyData() + ")");
            
        } catch (SQLException e)
        {
            System.err.println("Couldn't write config to database.");
            System.err.println(e.getMessage());
        }
        */
        netflowPack = new NetFlowPacket(con);
        ipfixPack = new IPFIXPacket(con);
        
    }
    

    
    private void openDatabase()
    {
        /*
        String url = "jdbc:mysql://" + ConfigParser.getDBServer() + "/" + ConfigParser.getDBName();
        
        try
        {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception e)
        {
            System.err.println("Failed to load MySQL driver.");
            return;
        }
        
        try
        {
            con = DriverManager.getConnection(url, ConfigParser.getDBUser(), ConfigParser.getDBPass());
            s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException e)
        {
            System.err.println("Failed to connect to database.");
            System.err.println(url + " " + ConfigParser.getDBUser() + " " + ConfigParser.getDBPass());
            System.err.println(e.getMessage());
            return;
        }
        */
        return;
    }
    
    public void startCollecting()
    {
        
        byte[] buf = new byte[MAX_UDP_SIZE + 4]; //add 4 bytes to be able to attach address
        //even to big packet
        byte[]         address;
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        int            length;
        
        new PacketReaderThread().start();
    
        if (1 > 0)
        {
            Calendar aggregationTime = new GregorianCalendar();
    
            aggregationTime.add(Calendar.DATE, 1);
            aggregationTime.set(Calendar.HOUR, 5);
            aggregationTime.set(Calendar.MINUTE, 0);
            aggregationTime.set(Calendar.SECOND, 0);
            aggregationTime.set(Calendar.AM_PM, Calendar.AM);
    
            System.out.println("Aggregation Time: " + aggregationTime.getTime());
            aggTimer = new Timer();
            aggTask = new AggregationTask();
    
            aggTimer.scheduleAtFixedRate(aggTask, aggregationTime.getTime(), 86400000l); //every 24h
    
            System.out.println("Scheduled Time: " + new Date(aggTask.scheduledExecutionTime()));
        }
        else
        {
            System.out.println("Aggregation disabled.");
        }
        
        
        while (true)
        {
            
            try
            {
                sock.receive(packet);
            } catch (IOException e)
            {
                
                System.out.println("Can't receive data from socket.");
                sock.close();
                System.exit(-1);
            }

//    the following to lines copy the source IP to the packet buffer so that this information is
            // available for the PacketReaderThread (don't like this solution but it works)
            //System.arraycopy(packet.getAddress().getAddress(), 0, buf, length=packet.getLength(), 4);
            //packet.setLength(length + 4); //length instead of packet.getLength to prevent one method call
            
            address = packet.getAddress().getAddress();  //HIER!!!!!
            length = packet.getLength();
            buf[length] = address[0];
            buf[length + 1] = address[1];
            buf[length + 2] = address[2];
            buf[length + 3] = address[3];
            udpBuf.push(packet.getData(), length + 4);
            
            //packet.setLength(MAX_UDP_SIZE+4);
        }
        
    }
    
    private FlowsetPacket getFlowset(byte[] buf, int bufLen)
    {
        
        int version = 0;
        
        //this needs to be changed
        if (bufLen - 4 < Nf9Header.getSize())
        {
            return null;
        }
        
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf));
        
        try
        {
            version = in.readUnsignedShort();
            in.close();
            
            
            switch (version)
            {
                
                case 9:
                    netflowPack.setContent(buf);
                    netflowPack.setSrcAddress((long) ByteBuffer.wrap(buf, bufLen - 4, 4).getInt() & 0xffffffffL);
                    return netflowPack;
                case 10:
                    ipfixPack.setContent(buf);
                    ipfixPack.setSrcAddress((long) ByteBuffer.wrap(buf, bufLen - 4, 4).getInt() & 0xffffffffL);
                    return ipfixPack;
                default:
                    return null;
            }
            
        } catch (IOException e)
        {
            System.out.println("Couldn't readShort.");
            return null;
        }
    }
    
    private class PacketReaderThread extends Thread
    {
        
        private byte[] buf = new byte[MAX_UDP_SIZE];
        //private Statement s = openDatabase();
        //private Connection con = openDatabase();
        
        public PacketReaderThread()
        {
    
   /*if (con==null) {
    sock.close();
    System.exit(-1);
   }*/
        }
        
        public void run()
        {
            
            FlowsetPacket flowPack;
            int           length;
            
            while (true)
            {
                
                length = udpBuf.pull(buf);
                
                if ((flowPack = getFlowset(buf, length)) == null)
                {
                
                
                }
                else
                {
                    try
                    {
                        flowPack.readContents();
                    } catch (IOException e)
                    {
                        System.err.println("Error reading flowset contents.");
                        return;
                    } catch (FlowFormatException e)
                    {
                        System.err.println("Error reading flowset contents.");
                        return;
                    }
                }
            }
        }
        
    }
    
    private class AggregationTask extends TimerTask
    {
        
        Aggregator agg = new Aggregator();
        
        public void run()
        {
            
            agg.start();
            
        }
    }void m
    private class UdpBuffer
    {
        
        private static final int BUFFER_LENGTH = MAX_UDP_SIZE * 500;
        private byte[] udpBuffer        = new byte[BUFFER_LENGTH];
        private int    count            = 0;
        private int    usedBufferLength = 0;
        private int    dropped          = 0;
        private Index[] idx = new Index[2000];
        
        public UdpBuffer()
        {
    
            for (int i = 0; i < idx.length; i++)
            {
                idx[i] = new Index();
            }
        }
        
        public synchronized void push(byte[] pkt, int length)
        {
            
            if ((count >= idx.length - 1) || (length + usedBufferLength > BUFFER_LENGTH))
            {
                dropped++;
                System.err.println(dropped + " packet(s) dropped.");
                return;
            }
            System.arraycopy(pkt, 0, udpBuffer, usedBufferLength, length);
            idx[count].offset = usedBufferLength;
            idx[count].length = length;
            count++;
            //System.out.println("rein(" + count + ")");
            usedBufferLength += length;
    
            if (count == 1)
            {
                notify();
            }
        }
        
        public synchronized int pull(byte[] buf)
        {
            
            if (count == 0)
            {
                try
                {
                    wait();
                } catch (InterruptedException e)
                {
                    System.err.println("Interrupted.");
                }
            }
            
            if (buf.length < idx[count - 1].length)
            {
                System.err.println("Buffer to small for UDP packet.");
                return 0;
            }
            
            System.arraycopy(udpBuffer, idx[count - 1].offset, buf, 0, idx[count - 1].length);
            usedBufferLength -= idx[count - 1].length;
            count--;
            //System.out.println("raus(" + count + ")");
            
            return idx[count].length;
        }
        
        private class Index
        {
            int offset;
            int length;
        }
    }
    
    public static ain(String[] args)
    {
        
        Collector col = new Collector();
        
        col.startCollecting();
        
    }
}
