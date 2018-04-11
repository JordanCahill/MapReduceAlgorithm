import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class adds the map functions to a thread pool to observe the time taken using different amounts of threads
 */
public class MapReduceWMapThreadPool {

    public static void main(String[] args) throws FileNotFoundException {

        // Get start time to compare different numbers of threads
        final long startTime = System.currentTimeMillis();
        Map<String, String> input = new ConcurrentHashMap<>();

        // Read in arguments
        for (int i = 1; i < args.length; i++) {
            File file = new File(args[i]);
            String name = file.getName();

            StringBuilder contentsSB = new StringBuilder((int) file.length());
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                contentsSB.append(scanner.nextLine() + "\n");
            }
            input.put(name, contentsSB.toString());
        }

        // Initialize hashmap to store output results
        final Map<String, Map<String, Integer>> output = new ConcurrentHashMap<>();

        int poolSize = Integer.parseInt(args[0]); // Store number of threads as user first argument

        ExecutorService ex = Executors.newFixedThreadPool(poolSize); // Initialize thread pool with x threads


        // MAP: --------------------------------------------

        final List<MappedItem> mappedItems = new LinkedList<>();

        // Call back to allow results to be added to mappedItems, note the synchronized keyword
        final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
            @Override
            public synchronized void mapDone(String file, List<MappedItem> results) {
                mappedItems.addAll(results);
            }
        };

        Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
        while (inputIter.hasNext()) {
            Map.Entry<String, String> entry = inputIter.next();
            final String file = entry.getKey();
            final String contents = entry.getValue();

            // Execute the thread pool
            ex.execute(() -> map(file, contents, mapCallback));
        }

        // Wait for thread pools to finish to calculate the time accurately
        ex.shutdown();
        while(!ex.isTerminated());

        // Calculate and print time taken
        final long timeTaken = System.currentTimeMillis() - startTime;
        System.out.println("Total Time taken with " + poolSize + " threads: " + timeTaken);

        // GROUP: -------------------------------------------------------------

        Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

        // Group all items in a LinkedList
        Iterator<MappedItem> mappedIter = mappedItems.iterator();
        while(mappedIter.hasNext()) {
            MappedItem item = mappedIter.next();
            String word = item.getWord();
            String file = item.getFile();
            List<String> list = groupedItems.get(word);
            if (list == null) {
                list = new LinkedList<String>();
                groupedItems.put(word, list);
            }
            list.add(file);
        }

        // REDUCE: --------------------------------------------------------------

        final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
            @Override
            public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
            }
        };

        List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

        Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
        while(groupedIter.hasNext()) {
            Map.Entry<String, List<String>> entry = groupedIter.next();
            final String word = entry.getKey();
            final List<String> list = entry.getValue();

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    reduce(word, list, reduceCallback);
                }
            });
            reduceCluster.add(t);
            t.start();
        }

        // wait for reducing phase to be over:
        for(Thread t : reduceCluster) {
            try {
                t.join();
            } catch(InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {

        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new ConcurrentHashMap<String,Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    public static interface MapCallback<E, V> {
        public void mapDone(E key, List<V> values);
    }
    public interface ReduceCallback<E, K, V> {
        void reduceDone(E e, Map<K, V> results);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }
}

