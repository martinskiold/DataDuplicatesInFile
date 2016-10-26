package com.martinskiold;

import java.nio.file.Path;
import java.nio.file.Paths;

// Version: QuickExitNoTime
public class Main {

    public static void main(String[] args) throws Exception {

        if(args.length != 0) {
            Path filePath = Paths.get(args[0]);

            ConcurrentDuplicateCheck dupCheck = new ConcurrentDuplicateCheck(filePath.toFile(), 8);
            //Creates 4 tasks that process 12 million bytes each.
            dupCheck.processAllBlocks(4, 12000000);
            //If execution reaches to this place there was no duplicate in the file. (Because: System.exit(0) if duplicates found)
            System.out.println("Duplicate not found.");
        }
        else
        {
            System.out.println("Please provide a file as argument to the program and make sure that the file resides in the same directory as the executable.");
        }
    }
}
