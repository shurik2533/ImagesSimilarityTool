/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.s2533.similarity;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import javax.imageio.ImageIO;
import org.imgscalr.Scalr;

/**
 *
 * @author shurik2533
 */
public class ImagesSimilarity {
  public boolean hasSimilar() {
    return false;
  }
  
  public static void main(String[] args) throws IOException {
    BufferedImage img = ImageIO.read(new File("/home/shurik2533/4.jpg"));
    BufferedImage thumbnail = Scalr.resize(img, Scalr.Method.SPEED, Scalr.Mode.FIT_EXACT,
               100, 100, Scalr.OP_GRAYSCALE);
    
    BufferedImage img2 = ImageIO.read(new File("/home/shurik2533/4.jpg"));
    BufferedImage thumbnail2 = Scalr.resize(img2, Scalr.Method.SPEED, Scalr.Mode.FIT_EXACT,
               100, 100, Scalr.OP_GRAYSCALE);
    System.out.println(getSimilarity(toByteArray(thumbnail), toByteArray(thumbnail2)));
  }
  
  /**
   * return similarity of two arrays in percents
   */
  public static double getSimilarity(byte[][] b1, byte[][] b2) {
    if (b1.length == 0 || b2.length == 0 || b1[0].length == 0 || b2[0].length == 0) {
      throw new IllegalArgumentException("One of the arrays dimensions is 0");
    }
    if (b1.length != b2.length || b1[0].length != b2[0].length) {
      throw new IllegalArgumentException("Arrays must be an equals size");
    }
    int total = 256 * b1.length * b1[0].length;

    double sum = 0;
    for (int i = 0; i < b1.length; i++) {
      for (int j = 0; j < b1[0].length; j++) {
        sum += Math.abs(b1[i][j]-b2[i][j]);
      }
    }
    return 100-sum/total*100;
  }
  
  private static byte[][] toByteArray(BufferedImage img) {
    if(img == null) {
      throw new IllegalArgumentException("Image is null");
    }
    final byte[][] result = new byte[img.getHeight()][img.getWidth()];
    for (int i = 0; i < result.length; i++) {
      for (int j = 0; j < result[0].length; j++) {
        int rgb = img.getRGB(j, i);
        result[i][j] = (byte) (rgb & 0xFF);
      }
    }
    return result;
  }
}
