import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

public class WordCntRun implements Runnable {
    private StringBuilder inputSb=null;
    private Map<String, Integer> mainMap;
    private Map<String, Integer> wordCnt=null;
    private BufferedReader reader;
    private ReentrantLock readerLock;
    private ReentrantLock mapLock;
    private final int chunkSize=102_4000;
    private char[] tempCharArr=new char[chunkSize];

    private WordCntRun(Map<String, Integer> _mainMap, BufferedReader _reader, ReentrantLock _readerLock, ReentrantLock _mapLock) {
        mainMap=_mainMap;
        reader=_reader;
        readerLock =_readerLock;
        mapLock=_mapLock;
    }

    // isDelims is used to check if a char is a \s, \r or \n
    private static boolean isDelims(char ch) {
        String str=" \r\n";
        for(char c:str.toCharArray()) {
            if(ch==c) {
                return true;
            }
        }
        return false;
    }

    // fetchSb() is called when the thread has already counted the whole chunk,
    // and then it will try to get the next chunk
    private void fetchSb() throws Exception {
        inputSb=new StringBuilder();
        wordCnt=new HashMap<>();
        int numReaded=reader.read(tempCharArr, 0, chunkSize);
        if(numReaded==-1) {
            return;
        }

        for(int i=0; i<numReaded; i++) {
            inputSb.append(tempCharArr[i]);
        }
        if(!isDelims(inputSb.charAt(inputSb.length()-1))) {
            int charValue = reader.read();
            while (charValue != -1 && !isDelims((char)charValue)) {
                inputSb.append((char)charValue);
                charValue=reader.read();
            }
        }
    }

    private void updateToMainMap() {
        for(Map.Entry<String, Integer> entry:wordCnt.entrySet()) {
            String word=entry.getKey();
            int cnt=entry.getValue();
            mainMap.put(word, mainMap.getOrDefault(word, 0)+cnt);
        }
    }

    public void run() {
        while(true) {
            // each thread is using the same BufferedReader so a lock is required here
            readerLock.lock();
            try {
                fetchSb();
            } catch (Exception e) {
                e.printStackTrace();
                inputSb=null;
                wordCnt=null;
                break;
            } finally {
                readerLock.unlock();
            }
            // if inputSb didn't get anything, then it is the EOF
            if (inputSb.length() == 0) {
                break;
            }
            int start = 0;
            for (int i = 0; i < inputSb.length(); i++) {
                if (isDelims(inputSb.charAt(i))) {
                    if (i - 1 >= start) {
                        String word = inputSb.substring(start, i);
                        wordCnt.put(word, wordCnt.getOrDefault(word, 0) + 1);
                    }
                    start = i + 1;
                }
            }
            // we might lose the last word if the end of the chunk is not a space
            if (!isDelims(inputSb.charAt(inputSb.length() - 1))) {
                String word = inputSb.substring(start);
                wordCnt.put(word, wordCnt.getOrDefault(word, 0) + 1);
            }
            // we use the same main HashMap for every thread to update to, so
            // a lock is required here for thread safe
            mapLock.lock();
            updateToMainMap();
            mapLock.unlock();
            inputSb = null;
            wordCnt=null;
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length!=2) {
            System.out.println("Not enough arguments. Usage: \"WordCntRun.java filepath threadNum\"");
            return;
        }
        long startTime=System.currentTimeMillis();
        int N=Integer.valueOf(args[1]);
        File file=new File(args[0]);
        BufferedReader reader=new BufferedReader(new FileReader(file));
        System.out.println("File size in bytes: "+file.length());
        List<Thread> threadList=new ArrayList<>();
        // minHeap is used here to get the top 100 words, the heap size is controlled
        // at 100 for lower time complexity
        PriorityQueue<Map.Entry<String, Integer>> minHeap=new PriorityQueue<>(
                (o1, o2)->!o1.getValue().equals(o2.getValue())
                        ?Integer.compare(o1.getValue(), o2.getValue())
                        :o2.getKey().compareTo(o1.getKey()));
        // this is the mainMap that each thread will update their counts to
        Map<String, Integer> mainMap=new HashMap<>();
        ReentrantLock readerLock=new ReentrantLock();
        ReentrantLock mapLock=new ReentrantLock();

        // start all the N threads
        for(int i=1; i<=N; i++) {
            Thread thread=new Thread(new WordCntRun(mainMap, reader, readerLock, mapLock));
            thread.start();
            threadList.add(thread);
        }

        for(Thread thread:threadList) {
            thread.join();
        }
        for(Map.Entry<String, Integer> entry:mainMap.entrySet()) {
            minHeap.add(entry);
            if(minHeap.size()==101) {
                minHeap.poll();
            }
        }
        long endTime=System.currentTimeMillis();
        System.out.println("Total Execution Time is: "+(endTime-startTime)/1000+" s");
        System.out.println("The Top 100 Words are: ");
        System.out.println(String.format("%30s%40s", "Word", "Frequency"));
        // a LinkedList is used here to print the results in minHeap in the right order
        LinkedList<Map.Entry<String, Integer>> resLst=new LinkedList<>();
        while(!minHeap.isEmpty()) {
            resLst.addFirst(minHeap.poll());
        }
        for(Map.Entry<String, Integer> entry:resLst) {
            System.out.println(String.format("%30s%40d", entry.getKey(), entry.getValue()));
        }
    }
}
