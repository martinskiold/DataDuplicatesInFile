package com.martinskiold;

import java.nio.file.Path;
import java.nio.file.Paths;

//Version: NormalExit
public class Main {

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();
        if(args.length != 0) {
            Path filePath = Paths.get(args[0]);

            ConcurrentDuplicateCheck dupCheck = new ConcurrentDuplicateCheck(filePath.toFile(), 8, startTime);

            String duplicate;
            if((duplicate = dupCheck.processAllBlocks(4, 1200000)) != null)
            {
                System.out.println("Duplicate found: " + duplicate);
            }
            else
            {
                System.out.println("Duplicate not found.");
            }

            System.out.println("Time passed " + ((double) (System.currentTimeMillis() - startTime)/1000));
        }
        else
        {
            System.out.println("Please provide a file as argument to the program and make sure that the file resides in the same directory as the executable.");
        }
    }
}
