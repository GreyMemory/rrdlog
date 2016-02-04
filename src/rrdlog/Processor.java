/*
 * Copyright(c) Anton Mazhurin & Nawwaf Kharma
 * 
 */
package rrdlog;

import java.util.ArrayList;

/**
 *
 * @author amazhurin
 */
public interface Processor {
   public void process(String host, String channel, ArrayList<Sample> samples);
}
