/*
 * Copyright(c) Anton Mazhurin & Nawwaf Kharma
 */
package rrdlog;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Reader from rrd files
 * @author amazhurin
 */
public class Reader extends Thread{
    class ChannelInfo{
        public ChannelInfo(String rrdFileName_){
            rrdFileName = rrdFileName_;
        }
        
        public String rrdFileName;
        public long lastTimestamp;
    }
    
    class Host {
        public String name;
        
        /**
         * RRD file per channel
         */
        public HashMap channelInfos = new HashMap();
        
        private ArrayList<String> executeCommand(String command) {
            ArrayList<String> result = new ArrayList<>();
            
            //System.out.println("Executing:" + command);
            
            StringBuilder output = new StringBuilder();
            Process p;
            try {
                p = Runtime.getRuntime().exec(command);
                p.waitFor();
                BufferedReader reader = 
                    new BufferedReader(new InputStreamReader(p.getInputStream()));

                String line;			
                while ((line = reader.readLine())!= null) {
                        result.add(line);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

            return result;

        }
        
        protected ArrayList<Sample> getSamplesFromRRDTool(String rrdFile, 
                String channel){
            ArrayList result = new ArrayList<>();
            
            String command = "rrdtool fetch " + rrdFile + " AVERAGE" 
                    //+ " --start -" + Integer.toString(rrdFetchTimeInMinutes) + "m"
                    ;
            
            //System.out.println(command);
            
            ArrayList<String> lines = executeCommand(command);
            //ArrayList<String> lines = new ArrayList<>();
            
            for(int i = 2; i < lines.size(); i++){
                String [] parts;
                parts = lines.get(i).split(" ");
                
                Sample sample = new Sample();
                try {
                    sample.timestamp = Integer.parseInt(parts[0].substring(0,
                            parts[0].length()-1));
                    sample.value = Float.parseFloat(parts[1]);
                    result.add(sample);
                } catch(Exception ex){
                }
                
            }
            
            return result;
        }
        
        public ArrayList<Sample> getSamples(Path dir, String channel){
            // get rrd file
            ArrayList result = new ArrayList();
            
            // get the current channel info object
            ChannelInfo channelInfo = (ChannelInfo) channelInfos.get(channel);
            
            // check the RRD file exists
            if(channelInfo == null || channelInfo.rrdFileName == null){
                System.out.println("ChannelInfo -> no rrd file");
                return result;
            }
            
            String rrd = dir + "/" + channelInfo.rrdFileName;
            File f = new File(rrd);
            if(!f.exists()) {
                System.out.println("File " + rrd + " does not exist.");
                return result;
            }
            
            // get fresh samples from RRD TOOL
            ArrayList<Sample> new_samples = getSamplesFromRRDTool(rrd, channel);
            for (Sample s : new_samples) {
                if(s.timestamp <= channelInfo.lastTimestamp)
                    continue;
                result.add(s);
                channelInfo.lastTimestamp = s.timestamp;
                //System.out.println("new sample ts="+s.timestamp);
            }
            
            return result;
        }
    }
    
    public String[] channels;
    private final String rrdFolder;
    
    /**
     * The time used in rrd fetch command
     */
    public int rrdFetchTimeInMinutes = 30;
    
    public Reader(String rrdFolder_, String[] channels_) throws Exception{
        channels = channels_;
        rrdFolder = rrdFolder_;
        Path fileDesctiption = Paths.get(rrdFolder + "/index.ngraph");
        
        //System.out.println("Index.ngraph : " + fileDesctiption.toString());
        
        create_hosts(fileDesctiption);
    }
    
    private ArrayList<Processor> processors = new ArrayList<Processor>();
    public void addProcessor(Processor processor){
        processors.add(processor);
    }
    
    private ArrayList<Host> hosts = new ArrayList<Host>();
    private void create_hosts(Path filePath) throws Exception {
        hosts.clear();
        Stream<String> stream = Files.lines(filePath, 
                StandardCharsets.US_ASCII);
        
        boolean host_line = false;
        int begins_counter = 0;
        boolean channel_line = false;
        Host host = null;
        String channel = "";
        for(Iterator<String> i = stream.iterator(); i.hasNext(); ) {
            String item = i.next();

            if(item.contains("{"))
                begins_counter++;
            if(item.contains("}"))
                begins_counter--;
            
            if(host_line){
                String[] parts = item.split("'", 3);
                if(parts.length > 2){
                    host = new Host();
                    host.name = parts[1];
                    host_line = false;
                    channel_line = false;
                    continue;
                }
            }
          
            if(begins_counter == 2){
                if(host != null && host.channelInfos.size() > 0){
                    hosts.add(host);
                }
                host = null;
                host_line = true;
                continue;
            }
          
            if(host == null)
                continue;
            
            if(channel_line == false){
                for(String c : channels){
                    if(item.contains(c)){
                        channel = c;
                        channel_line = true;
                        break;
                    }
                }
            }
            
            if(channel_line){
                if(item.contains("rrd_file")){
                    String[] parts = item.split("'");
                    if(parts.length > 3){
                        host.channelInfos.put(channel, new ChannelInfo(parts[3]));
                        host_line = false;
                        channel_line = false;
                        continue;
                    }
                }
            }
            
            if(item.contains(";") && host.channelInfos.size() > 0){
                hosts.add(host);
                host = null;
            }
        }       

        
    } 
    
    public void run(){
        
        //System.out.println("Reader.run() ...");
        
        while(true){
            try {
                for(Host host : hosts){
                    //if(!"chicago7".equals(host.name)) continue;
                
                    Path dir = Paths.get(rrdFolder + "/" + host.name + "/");
                    //System.out.println("Path to rrd:" + dir.toString());
                    
                    for(String channel : channels){
                        ArrayList<Sample> samples = host.getSamples(dir, channel);
                        if(samples != null && samples.size() > 0){
                            for(Processor p : processors){
                                p.process(host.name, channel, samples);
                            }
                        }
                    }
                }
                
                Thread.sleep(10000);
                
            } catch (InterruptedException e) {
                return;
            }
        }
    }

}
