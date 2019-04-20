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

    private static boolean isDelims(char ch) {
        String str=" \r\n";
        for(char c:str.toCharArray()) {
            if(ch==c) {
                return true;
            }
        }
        return false;
    }

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
            if (!isDelims(inputSb.charAt(inputSb.length() - 1))) {
                String word = inputSb.substring(start);
                wordCnt.put(word, wordCnt.getOrDefault(word, 0) + 1);
            }
            mapLock.lock();
            updateToMainMap();
            mapLock.unlock();
            inputSb = null;
            wordCnt=null;
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length!=2) {
            System.out.println(Runtime.getRuntime().maxMemory());
            System.out.println(Runtime.getRuntime().freeMemory());
            System.out.println("Not enough arguments. Usage: \"WordCntRun.java filepath threadNum\"");
            return;
        }
        long startTime=System.currentTimeMillis();
        int N=Integer.valueOf(args[1]);
        File file=new File(args[0]);
        BufferedReader reader=new BufferedReader(new FileReader(file));
        System.out.println(file.length());
        List<Thread> threadList=new ArrayList<>();
        PriorityQueue<Map.Entry<String, Integer>> minHeap=new PriorityQueue<>(
                (o1, o2)->!o1.getValue().equals(o2.getValue())
                        ?Integer.compare(o1.getValue(), o2.getValue())
                        :o2.getKey().compareTo(o1.getKey()));
        Map<String, Integer> mainMap=new HashMap<>();
        ReentrantLock readerLock=new ReentrantLock();
        ReentrantLock mapLock=new ReentrantLock();

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
        LinkedList<Map.Entry<String, Integer>> resLst=new LinkedList<>();
        while(!minHeap.isEmpty()) {
            resLst.addFirst(minHeap.poll());
        }
        for(Map.Entry<String, Integer> entry:resLst) {
            System.out.println(String.format("%30s%40d", entry.getKey(), entry.getValue()));
        }
    }
}
