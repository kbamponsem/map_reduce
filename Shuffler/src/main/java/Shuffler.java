import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Shuffler {
    public static void main(String[] args) {
        String dirname = Paths.get(args[0]).toAbsolutePath().toString();
        String machines = args[1];
        File directory = new File(dirname);

        for (File file : Objects.requireNonNull(directory.listFiles())) {
            shuffle(file.getAbsolutePath(), machines);
        }

    }

    static void shuffle(String filename, String machines) {
        Scanner scanner;
        HashMap<String, Vector<String>> hashMap = new HashMap<>(); // This holds the hash codes of all the keys

        try {
            scanner = new Scanner(new FileReader(filename));
            String line;
            while (scanner.hasNextLine()) {
                line = scanner.nextLine();
                String[] keyAndValue = line.split(" ");
                int fileHash = Math.abs(keyAndValue[0].hashCode());

                String key = String.valueOf(fileHash);
                Vector<String> list = new Vector<>();
                list.add(keyAndValue[0] + " " + keyAndValue[1]);
                if (hashMap.containsKey(key)) {
                    Vector<String> currentList = hashMap.get(key);
                    currentList.add(keyAndValue[0] + " " + keyAndValue[1]);
                    hashMap.put(key, currentList);
                } else
                    hashMap.put(String.valueOf(fileHash), list);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            hashMap.forEach((x, y) -> {
                try {
                    String localAddr = java.net.InetAddress.getLocalHost().toString().replace("/", "-");
                    String fileHashName = "/tmp/amponsem/shuffles/" + x + "-" + localAddr + ".txt";
                    StringBuilder output = new StringBuilder();
                    y.forEach(line -> output.append(line).append("\n"));

                    System.out.println(fileHashName);
                    Files.write(Paths.get(fileHashName), output.toString().getBytes(StandardCharsets.UTF_8));

                    // Exchange the right files
                    ArrayList<String> availableMachines = getMachinesAsList(machines);

                    String ip = availableMachines.get(Math.abs(Integer.parseInt(x)) % availableMachines.size());

                    copySimilarFiles(fileHashName, ip);

                } catch (IOException e) {
                    e.printStackTrace();
                }

            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static ArrayList<String> getMachinesAsList(String machines) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(machines));
        ArrayList<String> list = new ArrayList<>();
        String ip;

        while ((ip = bufferedReader.readLine()) != null) {
            list.add(ip);
        }
        return list;
    }

    static void copySimilarFiles(String filename, String ip) throws IOException {
        ProcessBuilder similarFiles = new ProcessBuilder("scp", filename, "amponsem"+"@"+ip+":/tmp/amponsem/shufflesReceived/");

        Process p = similarFiles.start();

        BufferedReader successStream = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));

        String results;

        while ((results = successStream.readLine()) != null) {
            System.out.println("[OUTPUT] Similar Files: " + results);
        }

        while ((results = errorStream.readLine()) != null) {
            System.out.println("[ERROR] Similar Files: " + results);
        }
        p.destroy();
    }


}
