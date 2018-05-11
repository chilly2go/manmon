package de.manmon.test.japes.nasty.collector;

public class Nf9Header {
    
    private short version;
    private short count;
    private int uptime;
    private int unixSecs;
    private int sequence;
    private int sourceID;
    
    public static int getSize() {
        
        return 32;
    }
    
}
