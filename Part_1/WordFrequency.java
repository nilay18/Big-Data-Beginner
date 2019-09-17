/*
 * Set the VM Parameter by using -Xmx5120M which will set the maximum heap size to 5120MB.
 * It is used for processing huge dataset like the 32GB text file
 */
import java.util.*;
import java.io.*;

public class WordFrequency {
    public static void main(String[] args) throws FileNotFoundException,IOException {
        // open the file
    	// Change the file name according to the dataset i.e Out_1GB for 1GB.
    	FileWriter fileWriter = new FileWriter("Out_1GB.txt");
    	
        PrintWriter printWriter = new PrintWriter(fileWriter);
        
        // Noting the start time
        printWriter.printf("Start Time: ");
        printWriter.print(java.time.LocalDateTime.now());
        printWriter.printf("\n");
        System.out.println("Before file Read:" +java.time.LocalDateTime.now());
        
        // Pointer to the source file, change it according to the location of dataset file present on disk.
    	File file = new File("C:\\Users\\nilay\\Downloads\\DataSet\\data_1GB.txt"); 
        Scanner input = new Scanner(file);
        System.out.println("Unsorted HashMap starts:" +java.time.LocalDateTime.now());
        
        // Initializing the HashMap
        Map<String, Integer> wordCount = new HashMap<String, Integer>();
        
        // Building the HashHap and increment the value if word already present in HashMap
        while (input.hasNext()) {
            String next = input.next();
            if (!wordCount.containsKey(next)) {
                wordCount.put(next, 1);
            } else {
                wordCount.put(next, wordCount.get(next)+1);
            }
        }
        input.close();
        System.out.println("Sorting of HashMap starts:" +java.time.LocalDateTime.now());
        
        // Initializing the LinkedHashMap and storing the reversed sorted data into it.       
        LinkedHashMap<String, Integer> reverseWordCounts = new LinkedHashMap<>();
        wordCount.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseWordCounts.put(x.getKey(), x.getValue()));
        int limit = 100;
        System.out.println("Sorting Completed:" +java.time.LocalDateTime.now());
        
        // Printing the first 100 values of the LinkedHashMap
        for (String word : reverseWordCounts.keySet()) {
        	if(limit >0)
        	{
	            int count = reverseWordCounts.get(word);
	            printWriter.printf(count + "\t" + word+"\n");
	            limit --;
        	}
        	else
        	{
        		 printWriter.printf("End Time: ");
        	     printWriter.print(java.time.LocalDateTime.now());
        	     break;
        	}
        }
        System.out.println("Writing to file completed:" +java.time.LocalDateTime.now());
        printWriter.close();
    }                      
}