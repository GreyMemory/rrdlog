/*
 * Copyright(c) Anton Mazhurin & Nawwaf Kharma
 * 
 */
package rrdlog;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * @author amazhurin
 */
public class ProcessorLog implements Processor{
    
    class HostLog {
        HashMap<String, Long> timestamps = new HashMap();
    }
    
    private final String directory;
    private final String[] channels;
    
    public ProcessorLog(String directory_, String[] channels_){
        directory = directory_;
        channels = channels_;
    }
    
    private int getChannelIndex(String channel){
        for(int i = 0; i < channels.length; i++)
            if(channels[i] == null ? channel == null : channels[i].equals(channel))
                return i;
        return 0;
    }
    
    private final HashMap hosts = new HashMap();
    
    public String tail( String file_name ) {
        File file = new File(file_name);
        RandomAccessFile fileHandler = null;
        try {
            fileHandler = new RandomAccessFile( file, "r" );
            long fileLength = fileHandler.length() - 1;
            StringBuilder sb = new StringBuilder();

            for(long filePointer = fileLength; filePointer != -1; filePointer--){
                fileHandler.seek( filePointer );
                int readByte = fileHandler.readByte();

                if( readByte == 0xA ) {
                    if( filePointer == fileLength ) {
                        continue;
                    }
                    break;

                } else if( readByte == 0xD ) {
                    if( filePointer == fileLength - 1 ) {
                        continue;
                    }
                    break;
                }

                sb.append( ( char ) readByte );
            }

            String lastLine = sb.reverse().toString();
            return lastLine;
        } catch( java.io.FileNotFoundException e ) {
            //e.printStackTrace();
            return null;
        } catch( java.io.IOException e ) {
            e.printStackTrace();
            return null;
        } finally {
            if (fileHandler != null )
                try {
                    fileHandler.close();
                } catch (IOException e) {
                    /* ignore */
                }
        }
    }    
    
    private String get_host_file_name(String host, String channel){
        return directory + "/" + host + "_" +channel + ".csv";
    }
    
    @Override
    public void process(String host, String channel, ArrayList<Sample> samples){
        int channelIndex = getChannelIndex(channel);
        
        HostLog hostLog;
        hostLog = null;
        if(hosts.containsKey(host))
            hostLog = (HostLog)hosts.get(host);
        else{
            hostLog = new HostLog();
            hosts.put(host, hostLog);
        }

        long last_timestamp = 0;
        if(!hostLog.timestamps.containsKey(channel)){
            String last_entry;
            last_entry = tail(get_host_file_name(host, channel));
            if(last_entry != null){
                String [] parts = last_entry.split(",");
                last_timestamp = Long.parseLong(parts[0]);
                hostLog.timestamps.put(channel, last_timestamp);
                
            }
        } else {
            last_timestamp = hostLog.timestamps.get(channel);
        }
        
        // process new samples
        for(Sample newSample : samples){
            if(newSample.timestamp <= last_timestamp)
                continue;
            
            hostLog.timestamps.put(channel, newSample.timestamp);
            try
            {
                FileWriter writer;
                String filename = get_host_file_name(host, channel);
                //System.out.println("Writing to :" + filename);
                        
                writer = new FileWriter(filename, true);

                PrintWriter printWriter = new PrintWriter(writer);
                
                printWriter.printf("%d,", newSample.timestamp);
                
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
 
                System.out.printf("%s: %d,", 
                        dateFormat.format(new Date()),
                        newSample.timestamp);
                        
                printWriter.printf("%f", newSample.value);
                System.out.printf("%f,", newSample.value);
                
                printWriter.println();
                System.out.println(host + "," + channel);

                //Close writer
                writer.close();
            } catch(Exception e)
            {
                e.printStackTrace();
            }            
        }
        
    }
}
