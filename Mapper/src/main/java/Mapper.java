import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Vector;

public class Mapper {
    static String username;

    public static void main(String[] args) {
        String filename = args[0];
        username = args[1];
        int index = Integer.parseInt(args[0].split("-")[1].split("\\.")[0]);

        map(filename, index);
    }

    static String getBaseDirectory(String subdir) {
        return String.format("/tmp/%s/%s", username, subdir);
    }

    static void map(String filename, int index) {
        StringBuilder output = new StringBuilder();

        BufferedReader bufferedReader;
        Vector<String> mappedElements = new Vector<>();

        try {
            bufferedReader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] words = line.split(" ");

                for (String word : words) {
                    mappedElements.add(word + " " + 1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            mappedElements.forEach((x) -> {
                output.append(x).append("\n");
            });

            String unsortedMapFilePath = String.format("%s/UM-" + index + ".txt", getBaseDirectory("maps"));
            System.out.println(unsortedMapFilePath);

            Files.write(Paths.get(unsortedMapFilePath), output.toString().getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
