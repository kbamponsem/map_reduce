package processbuilder;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class CommandRunner {
    public static String username;
    public static String BUILD_PATH;

    public static void main(String[] args) throws IOException, InterruptedException {
        String ip = args[0];
        int index = Integer.parseInt(args[1]);
        String workingMachines = args[2];
        String projectPath = args[3];
        BUILD_PATH = projectPath + "/build";
        username = args[4];

        ArrayList<String> splits = new ArrayList<>();

        File splitsDirectory = new File(BUILD_PATH + "/splits");

        System.out.println("Running command on [" + ip + "] on directory " + username);

        for (File file : Objects.requireNonNull(splitsDirectory.listFiles())) {
            splits.add(file.getAbsolutePath());
        }
        makeDir(ip, "shufflesReceived");
        // makeDir(ip, "splits");
        // makeDir(ip, "maps");
        // makeDir(ip, "shuffles");
        // makeDir(ip, "reduces");

        // copySplits(splits.stream().sorted().collect(Collectors.toCollection(ArrayList::new)),
        // index, ip);
        // sendMachinesList(ip, workingMachines);

        // double startTime, endTime, total = 0;

        // startTime = System.currentTimeMillis();
        // runMapper(ip, index);
        // endTime = System.currentTimeMillis();

        // System.out.println("[" + ip + "] " + "[MAP] [TIME-TAKEN]: " + (endTime -
        // startTime) + " ms");
        // total += endTime - startTime;

        // startTime = System.currentTimeMillis();
        // runShuffler(ip);
        // endTime = System.currentTimeMillis();
        // System.out.println("[" + ip + "] " + "[SHUFFLE] [TIME-TAKEN]: " + (endTime -
        // startTime) + " ms");
        // total += endTime - startTime;

        // startTime = System.currentTimeMillis();
        // runReducer(ip);
        // endTime = System.currentTimeMillis();
        // System.out.println("[" + ip + "] " + "[REDUCE] [TIME-TAKEN]: " + (endTime -
        // startTime) + " ms");
        // total += endTime - startTime;

        // System.out.println("[TOTAL] [TIME-TAKEN]: " + total + " ms");

        // copyReducesToLocal(ip, projectPath);

    }

    static void copyReducesToLocal(String ip, String projectPath) throws IOException {
        ProcessBuilder slave = new ProcessBuilder("scp", "-r",
                username + "@" + ip + String.format(":/tmp/%s/reduces/*", username),
                projectPath + "/output");

        Process p = slave.start();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));

        String line;

        while ((line = bufferedReader.readLine()) != null) {
            System.out.println("[COPY-REDUCES] [OUTPUT]: " + line);
        }

        InputStream errorStream = p.getErrorStream();
        BufferedReader bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream));

        String errorLine;

        while ((errorLine = bufferedErrorReader.readLine()) != null) {
            System.out.println("[COPY-REDUCES] [ERROR]: " + errorLine);
        }

        p.destroy();
    }

    static void runShuffler(String ip) throws IOException {
        ProcessBuilder slave = new ProcessBuilder("ssh", username + "@" + ip,
                String.format("cd /tmp/%s; java -Xms4G -Xmx4G -jar shuffler-1.0-SNAPSHOT.jar maps/ machines.txt",
                        username));

        Process p = slave.start();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));

        String line;

        while ((line = bufferedReader.readLine()) != null) {
            System.out.println("[RUN-SHUFFLER] [OUTPUT]: " + line);
        }

        InputStream errorStream = p.getErrorStream();
        BufferedReader bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream));

        String errorLine;

        while ((errorLine = bufferedErrorReader.readLine()) != null) {
            System.out.println("[RUN-SHUFFLER] [ERROR]: " + errorLine);
        }

        p.destroy();
    }

    static void runReducer(String ip) throws IOException {
        ProcessBuilder slave = new ProcessBuilder("ssh", username + "@" + ip,
                String.format("cd /tmp/%s; java -Xms4G -Xmx4G -jar reducer-1.0-SNAPSHOT.jar shufflesReceived/",
                        username));

        Process p = slave.start();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));

        String line;

        while ((line = bufferedReader.readLine()) != null) {
            System.out.println("[SUCCESS] OUTPUT: " + line);
        }

        InputStream errorStream = p.getErrorStream();
        BufferedReader bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream));

        String errorLine;

        while ((errorLine = bufferedErrorReader.readLine()) != null) {
            System.out.println("[ERROR] OUTPUT: " + errorLine);
        }

        p.destroy();
    }

    static void sendMachinesList(String ip, String machines) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("scp", machines,
                String.format("%s@%s:/tmp/%s", username, ip, username));

        Process p = processBuilder.start();

        BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String errorLine;

        try {
            while ((errorLine = errorStream.readLine()) != null) {
                System.out.println(errorLine);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                errorStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    static void runMapper(String ip, int index) throws IOException {
        ProcessBuilder slave = new ProcessBuilder("ssh", username + "@" + ip,
                String.format("cd /tmp/%s; java -Xms2048m -Xmx2048m -jar mapper-1.0-SNAPSHOT.jar splits/S-%s.txt",
                        username, index),
                "exit");

        Process p = slave.start();

        readLines(p);
    }

    static void copySplits(ArrayList<String> splits, int index, String ip) throws IOException, InterruptedException {
        System.out.println("Copying splits...");
        ProcessBuilder processBuilder = new ProcessBuilder("scp", splits.get(index),
                String.format("username@%s:/tmp/%s/splits", username, ip, username));

        Process p = processBuilder.start();

        readLines(p);
    }

    private static void readLines(Process p) throws IOException {

        try (Scanner scanner = new Scanner(p.getInputStream());) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                System.out.println(line);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        try (Scanner scanner = new Scanner(p.getErrorStream())) {

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                System.out.println(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        p.destroy();
    }

    static String getBaseDirectory(String subdir) {
        return String.format("/tmp/%s/%s", username, subdir);
    }

    static void makeDir(String ip, String name) throws IOException {

        ProcessBuilder mkdirBuilder = new ProcessBuilder("bash", "-c",
                "ssh -o ConnectTimeout=2 " + username + "@" + ip + " ls " + getBaseDirectory(""));
        System.out.println(Arrays.toString(mkdirBuilder.command().toArray()));
        Process mkdirProcess = mkdirBuilder.start();

        try (BufferedReader mkdirErrorStream = new BufferedReader(
                new InputStreamReader(mkdirProcess.getErrorStream()));) {
            String errorLine;
            while ((errorLine = mkdirErrorStream.readLine()) != null) {
                System.out.println("Error: " + errorLine);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedReader success = new BufferedReader(
                new InputStreamReader(mkdirProcess.getInputStream()));) {
            String errorLine;
            while ((errorLine = success.readLine()) != null) {
                System.out.println("Success: " + errorLine);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        mkdirProcess.destroy();

    }

}
