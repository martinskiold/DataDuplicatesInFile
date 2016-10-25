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
    private long startTime;
    private int lineSizeBytes;
    private ExecutorService es;

    public ConcurrentDuplicateCheck(File file, int lineSizeBytes, long startTime)
    {
        this.file = file;
        this.lineSizeBytes = lineSizeBytes;
        this.hMap = new ConcurrentHashMap<String, Boolean>();
        this.startTime = startTime;
    }

    public String processAllBlocks(int threadCount, int processBlockSize) throws Exception
    {
        /*
        * Adjusts the size of each processblock so that it is a multiple of @lineSizeBytes, and at least @lineSizeBytes.
        * (Each line in file consists of @lineSizeBytes, and therefor the processblocks must be multiples of this value
        * in order to avoid that some lines being cut in half.
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
        ArrayList<Callable<String>> tasks = new ArrayList<Callable<String>>(taskCount);
        for (int i = 0; i<taskCount; i++) {
            tasks.add(processBlockJob(i * processBlockSize, Math.min(file.length(), (i+1)*processBlockSize)));
        }
        es = Executors.newFixedThreadPool(threadCount);
        List<Future<String>> results = es.invokeAll(tasks);

        if(!es.isShutdown())
        {
            es.shutdown();
        }

        for (Future<String> result:results){
            String res = result.get();
            //System.out.println(res);
            if(res != null)
            {
                //If duplicate
                return res;
            }
        }

        //If no duplicate
        return null;
    }

    public Callable<String> processBlockJob(final long start, final long end)
    {
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                return processBlock(start, end);
            }
        };
    }

    public String processBlock(long start, long end) throws Exception
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
                es.shutdownNow();
                return line;

                //Or: exit program directly
                //System.exit(0);
            }

            //Print contents of each line and corresponding processblock
            //System.out.println(line + "      from " + start + " to " + end);

            byteCount += lineSizeBytes;
        }

        is.close();
        return null;
    }

}
