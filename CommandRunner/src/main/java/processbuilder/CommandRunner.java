package processbuilder;

import com.sun.tools.jdeprscan.scan.Scan;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class CommandRunner {
    public static void main(String[] args) throws IOException, InterruptedException {
        String ip = args[0];
        int index = Integer.parseInt(args[1]);
        String workingMachines = args[2];
        String projectPath = args[3];

        ArrayList<String> splits = new ArrayList<>();

        File splitsDirectory = new File(projectPath + "/splits");

        for (File file : Objects.requireNonNull(splitsDirectory.listFiles())) {
            splits.add(file.getAbsolutePath());
        }

        System.out.println(Arrays.toString(splits.stream().sorted().toArray()));

        makeDir(ip, "shufflesReceived");
        makeDir(ip, "splits");
        makeDir(ip, "maps");
        makeDir(ip, "shuffles");
        makeDir(ip, "reduces");

        copySplits(splits.stream().sorted().collect(Collectors.toCollection(ArrayList::new)), index, ip);
        sendMachinesList(ip, workingMachines);

        double startTime, endTime, total = 0;

        startTime = System.currentTimeMillis();
        runMapper(ip, index);
        endTime = System.currentTimeMillis();

        System.out.println("[" + ip + "] " + "[MAP] [TIME-TAKEN]: " + (endTime - startTime) + " ms");
        total += endTime - startTime;

        startTime = System.currentTimeMillis();
        runShuffler(ip);
        endTime = System.currentTimeMillis();
        System.out.println("[" + ip + "] " + "[SHUFFLE] [TIME-TAKEN]: " + (endTime - startTime) + " ms");
        total += endTime - startTime;


        startTime = System.currentTimeMillis();
        runReducer(ip);
        endTime = System.currentTimeMillis();
        System.out.println("[" + ip + "] " + "[REDUCE] [TIME-TAKEN]: " + (endTime - startTime) + " ms");
        total += endTime - startTime;

        System.out.println("[TOTAL] [TIME-TAKEN]: " + total + " ms");

        copyReducesToLocal(ip, projectPath);

    }

    static void copyReducesToLocal(String ip, String projectPath) throws IOException {
        ProcessBuilder slave = new ProcessBuilder("scp", "-r", "amponsem@" + ip + ":/tmp/amponsem/reduces/*", projectPath + "/output");

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
        ProcessBuilder slave = new ProcessBuilder("ssh", "amponsem@" + ip, "cd /tmp/amponsem; java -Xms4G -Xmx4G -jar shuffler-1.0-SNAPSHOT.jar maps/ machines.txt");

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
        ProcessBuilder slave = new ProcessBuilder("ssh", "amponsem@" + ip, "cd /tmp/amponsem; java -Xms4G -Xmx4G -jar reducer-1.0-SNAPSHOT.jar shufflesReceived/");

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
        ProcessBuilder processBuilder = new ProcessBuilder("scp", machines, "amponsem@" + ip + ":/tmp/amponsem");

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
        ProcessBuilder slave = new ProcessBuilder("ssh", "amponsem@" + ip, "cd /tmp/amponsem; java -Xms2048m -Xmx2048m -jar mapper-1.0-SNAPSHOT.jar splits/S-" + index + ".txt ", "exit");

        Process p = slave.start();

        readLines(p);
    }

    static void copySplits(ArrayList<String> splits, int index, String ip) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("scp", splits.get(index), "amponsem@" + ip + ":/tmp/amponsem/splits");

        Process p = processBuilder.start();

        readLines(p);
    }

    private static void readLines(Process p) throws IOException {

        Scanner scanner = new Scanner(p.getInputStream());
        while (scanner.hasNextLine()){
            String line = scanner.nextLine();
            System.out.println(line);
        }

        scanner = new Scanner(p.getErrorStream());

        while (scanner.hasNextLine()){
            String line = scanner.nextLine();
            System.out.println(line);
        }

        p.destroy();
    }

    static void makeDir(String ip, String name) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "ls /tmp/amponsem/" + name);

        Process p = processBuilder.start();

        BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String errorLine;
        try {
            while (errorStream.readLine() != null) {
                ProcessBuilder mkdirBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "mkdir -p /tmp/amponsem/" + name);


                Process mkdirProcess = mkdirBuilder.start();

                BufferedReader mkdirErrorStream = new BufferedReader(new InputStreamReader(mkdirProcess.getErrorStream()));

                while ((errorLine = mkdirErrorStream.readLine()) != null) {
                    System.out.println("Error: " + errorLine);
                }
                mkdirProcess.destroy();
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
        p.destroy();
    }

}
