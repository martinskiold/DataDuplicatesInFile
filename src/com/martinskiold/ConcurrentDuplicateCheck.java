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
    private ExecutorService es;

    public ConcurrentDuplicateCheck(File file, int lineSizeBytes)
    {
        this.file = file;
        this.lineSizeBytes = lineSizeBytes;
        this.hMap = new ConcurrentHashMap<String, Boolean>();
    }

    public boolean processAllBlocks(int threadCount, int processBlockSize) throws Exception
    {
        /*
        * Adjusts the size of each processblock so that it is a multiple of @lineSizeBytes, and at least @lineSizeBytes.
        * (Each line in the file consists of @lineSizeBytes bytes, and therefore the processblocks must be multiples of
        * this value in order to avoid that some lines is cut in half.
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
        es = Executors.newFixedThreadPool(threadCount);
        List<Future<Boolean>> results = es.invokeAll(tasks);

        if(!es.isShutdown())
        {
            es.shutdown();
        }

        for (Future<Boolean> result:results){
            //System.out.println(res);
            if(result.get())
            {
                //If duplicates
                return true;
            }
        }

        //If no duplicates
        return false;
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

    public boolean processBlock(long start, long end) throws Exception
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
                return true;
            }

            //Print contents of each line and corresponding processblock
            //System.out.println(line + "      from " + start + " to " + end);

            byteCount += lineSizeBytes;
        }

        is.close();
        return false;
    }

}
