package com.sertac.example;

import java.io.IOException;
import java.io.Writer;
import java.util.Random;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class InputDataGenerator {
    
  private static final Random random = new Random();
    
  private static final int DEFAULT_LIMIT = 100000;
  private static final int DEFAULT_STRING_LENGTH = 10;
  private static final String alphabet="abcdefghijklmnopqrstuvwxyz0123456789";
  private static final InputDataGenerator instance = new InputDataGenerator();
  private static final Logger logger = LogManager.getLogger(InputDataGenerator.class);
  public static final char FIELD_DELIM_CHAR = '~';
  private InputDataGenerator(){} 
  
  public static InputDataGenerator getInstance(){
    return instance;
  }
  
  
  public void fillData(Class[] types, Writer writer){
    fillData(types, writer, DEFAULT_LIMIT);
  }
  
  public void fillData(Class[] types, Writer writer, char delimiter){
    fillData(types, writer, DEFAULT_LIMIT, delimiter);
  }
  
  public void fillData(Class[] types, Writer writer, int numOfLines){
    fillData(types, writer, numOfLines, FIELD_DELIM_CHAR);
  }

  public void fillData(Class[] types, Writer writer, int numOfLines, char delimiter) {
    try {
      for(int i=0; i<numOfLines; i++){
        writer.write(setupLine(types, delimiter));
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    } finally{
      try {
        writer.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        logger.error(e.getMessage());
      }
    }
  }

  private String setupLine(Class[] types, char delim) {
    // TODO Auto-generated method stub
    StringBuffer lineBuffer = new StringBuffer();
    lineBuffer.append(getRandChar());
    lineBuffer.append(FIELD_DELIM_CHAR);
    for(Class type:types){
      lineBuffer.append(getRandValue(type));
      lineBuffer.append(delim);
    }
    lineBuffer.deleteCharAt(lineBuffer.length()-1);
    lineBuffer.append('\n');
    return lineBuffer.toString();
  }

  private char getRandChar() {
    // TODO Auto-generated method stub
    return alphabet.charAt(random.nextInt(alphabet.length()-10));
  }

  private String getRandValue(Class type) {
    String retVal = null;
    if(type.equals(String.class)){
      StringBuffer sb = new StringBuffer();
      for(int i=0; i<DEFAULT_STRING_LENGTH; i++){
        sb.append(alphabet.charAt(random.nextInt(alphabet.length())));
      }
      retVal = sb.toString();
    } else if(type.equals(Integer.class)) {
      retVal = Integer.toString(random.nextInt(DEFAULT_LIMIT));
    } else if (type.equals(Float.class)){
      retVal = Float.toString(random.nextFloat());
    } else if (type.equals(Double.class)){
      retVal = Double.toString(random.nextDouble());
    }
    

    return retVal;
  }

}
