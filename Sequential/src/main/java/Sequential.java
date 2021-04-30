import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;

public class Sequential {
    public static void main(String[] args) throws IOException {
        String file = args[0];
        Scanner scanner = new Scanner(new FileReader(file));

        Vector<String> allWords = new Vector<>();
        HashMap<String, Long> mapReduce = new HashMap<>();

        while (scanner.hasNextLine()) {
            allWords.addAll(Arrays.asList(scanner.nextLine().split(" ")));
        }
        double start = System.currentTimeMillis();
        allWords.forEach(x->{
            mapReduce.put(x,allWords.stream().filter(x::equals).count());
        });
        double finish = System.currentTimeMillis();

        StringBuilder output = new StringBuilder();

        mapReduce.forEach((x,y)->{
            output.append(x).append(", ").append(y).append("\n");
        });

        System.out.println(output);
        System.out.println("[TIME-TAKEN] "+(finish-start) +" ms");
    }
}
