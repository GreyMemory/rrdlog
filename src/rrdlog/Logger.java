/*
 Copyright(c) Anton Mazhurin & Nawwaf Kharma
To run it in the background:
nohup ./logger.sh &

ps -A 

kill -1 pid
 */
package rrdlog;

import java.util.Scanner;

/**
 *
 * @author amazhurin
 */
public class Logger {
    
            
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.println("rrdlog (c) Anton Mazhurin");
        
        if(args.length < 1){
            System.out.println("The first argument should be the path to RRD folder");
            return;
        }
        
        String[] channels = {
            "AGGTCP",
            "AGG_CONN"
            //"TCPTRAFFIC",
            //"CONNECTIONS"
        };
        
        try{
            Reader reader = new Reader(args[0], channels);
            
            ProcessorLog logger = new ProcessorLog(args[1],channels);
            reader.addProcessor(logger);
            
            reader.start();
            
            int i = in.nextInt();
            System.out.println("interrupting...");
            reader.interrupt();
            reader.join();
            System.out.println("interrupted.");
            
        } catch(Exception e) {
            e.printStackTrace();
        }

    }

}
