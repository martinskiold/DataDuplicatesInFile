package com.martinskiold;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by martinskiold on 10/24/16.
 */
public class ConcurrentDuplicateCheck {
    private File file;
    private ConcurrentHashMap<String, Boolean> hMap;
    private int lineSizeBytes;

    public ConcurrentDuplicateCheck(File file, int lineSizeBytes)
    {
        this.file = file;
        this.lineSizeBytes = lineSizeBytes;
        this.hMap = new ConcurrentHashMap<String, Boolean>();
    }

    public void processAllBlocks(int threadCount, int processBlockSize) throws Exception
    {
        /*
        * Adjusts the size of each processBlock so that it is a multiple of @lineSizeBytes, and at least @lineSizeBytes.
        * (Each line in the provided file should consist of @lineSizeBytes bytes, and therefore the processblocks must
        * be multiples of this value in order to avoid that some lines is cut in half.
        */
        processBlockSize = (processBlockSize/lineSizeBytes);
        if(processBlockSize != 0)
        {
            processBlockSize = processBlockSize * lineSizeBytes;
        }
        else
        {
            processBlockSize = lineSizeBytes;
        }

        int taskCount = (int) ((file.length() + processBlockSize - 1) / processBlockSize);
        ArrayList<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(taskCount);
        for (int i = 0; i<taskCount; i++) {
            tasks.add(processBlockJob(i * processBlockSize, Math.min(file.length(), (i+1)*processBlockSize)));
        }
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        List<Future<Boolean>> results = es.invokeAll(tasks);

        es.shutdown();

        //Print returned values from tasks.
        //for (Future<String> result:results){
        //    System.out.println(res);
        //}
    }

    public Callable<Boolean> processBlockJob(final long start, final long end)
    {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return processBlock(start, end);
            }
        };
    }

    public Boolean processBlock(long start, long end) throws Exception
    {
        InputStream is = new FileInputStream(file);
        is.skip(start);


        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String line = null;
        long byteCount = start;
        while(byteCount < end && (line = in.readLine()) != null)
        {
            //Check for duplicates
            if(hMap.put(line, true) != null)
            {
                System.out.println("Duplicate found.");
                System.exit(0);
            }

            //Print contents of each line and corresponding processblock
            //System.out.println(line + "      from " + start + " to " + end);

            byteCount += lineSizeBytes;
        }

        is.close();
        return false;
    }

}
