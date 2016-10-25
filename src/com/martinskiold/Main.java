package com.martinskiold;

import java.nio.file.Path;
import java.nio.file.Paths;

// Version: QuickExit
public class Main {

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();
        if(args[0] != null) {
            Path filePath = Paths.get(args[0]);

            ConcurrentDuplicateCheck dupCheck = new ConcurrentDuplicateCheck(filePath.toFile(), 8, startTime);
            //Creates 4 tasks that process 12 million bytes each.
            dupCheck.processAllBlocks(4, 12000000);

            System.out.println("Duplicate not found.");
            System.out.println("Time passed " + ((double) (System.currentTimeMillis() - startTime)/1000));
        }
        else
        {
            System.out.println("Please provide the file as an argument to the program and make sure that the file resides in the same directory as the executable.");
        }
    }
}
