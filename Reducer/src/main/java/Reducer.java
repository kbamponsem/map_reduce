import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Vector;

public class Reducer {
    public static void main(String[] args) throws IOException {
        String dirname = Paths.get(args[0]).toAbsolutePath().toString();
        System.out.println(dirname);
        File directory = new File(dirname);

        Vector<String> uniqueIds = new Vector<>();

        for (File file : Objects.requireNonNull(directory.listFiles())) {
            if (file.getName().contains("-")) {
                String id = file.getName().split("-")[0];

                if (!uniqueIds.contains(id)) {
                    uniqueIds.add(id);
                }

            }
        }

        for (String id : uniqueIds) {
            reduce(id, Objects.requireNonNull(directory.listFiles(x -> x.getName().contains(id))));
        }

    }

    static void reduce(String id, File[] commonFiles) throws IOException {
        int count = 0;
        for (File file : commonFiles) {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line;
            String key = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] keyValue = line.split(" ");
                count += Integer.parseInt(keyValue[1]);
                key = keyValue[0];
            }
            try {
                String outputFile = "/tmp/amponsem/reduces/" + id + ".txt";

                Files.write(Paths.get(outputFile), (key + " " + count + "\n").getBytes(StandardCharsets.UTF_8));
                System.out.println(key + " " + count);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }
}
