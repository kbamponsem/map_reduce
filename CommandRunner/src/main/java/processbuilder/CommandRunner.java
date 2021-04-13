package processbuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

public class CommandRunner {
    public static void main(String[] args) throws IOException, InterruptedException {
        String ip = args[0];
        int index = Integer.parseInt(args[1]);
        String workingMachines = args[2];
        String projectPath = args[3];

        ArrayList<String> splits = new ArrayList<>();

        File splitsDirectory = new File(projectPath+"/splits");

        for (File file : Objects.requireNonNull(splitsDirectory.listFiles()))
        {
            splits.add(file.getAbsolutePath());
        }

        System.out.println(Arrays.toString(splits.stream().sorted().toArray()));


        makeShufflesReceivedDir(ip);
        makeSplitsDir(ip);
        makeMapsDir(ip);
        makeShufflesDir(ip);
        makeReducesDir(ip);

        copySplits(splits.stream().sorted().collect(Collectors.toCollection(ArrayList::new)), index, ip);
        sendMachinesList(ip, workingMachines);

        double startTime, endTime, total= 0;

        startTime = System.currentTimeMillis();
        runMapper(ip, index);
        endTime = System.currentTimeMillis();

        System.out.println("["+ip+"] "+"[MAP] [TIME-TAKEN]: "+ (endTime - startTime)+" ms");
        total += endTime - startTime;

        startTime = System.currentTimeMillis();
        runShuffler(ip);
        endTime = System.currentTimeMillis();
        System.out.println("["+ip+"] "+"[SHUFFLE] [TIME-TAKEN]: "+ (endTime - startTime)+" ms");
        total += endTime - startTime;


        startTime = System.currentTimeMillis();
        runReducer(ip);
        endTime = System.currentTimeMillis();
        System.out.println("["+ip+"] "+"[REDUCE] [TIME-TAKEN]: "+ (endTime - startTime)+" ms");
        total += endTime - startTime;

        System.out.println("[TOTAL] [TIME-TAKEN]: "+ total +" ms");

        copyReducesToLocal(ip, projectPath);

    }

    static void copyReducesToLocal(String ip, String projectPath) throws IOException {
        ProcessBuilder slave = new ProcessBuilder("scp", "-r", "amponsem@"+ip+":/tmp/amponsem/reduces/*", projectPath+"/output");

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
        ProcessBuilder slave = new ProcessBuilder("ssh", "amponsem@" + ip, "cd /tmp/amponsem; java -jar shuffler-1.0-SNAPSHOT.jar maps/ machines.txt");

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
        ProcessBuilder slave = new ProcessBuilder("ssh", "amponsem@" + ip, "cd /tmp/amponsem; java -jar reducer-1.0-SNAPSHOT.jar shufflesReceived/");

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

        while ((errorLine = errorStream.readLine()) != null) {
            System.out.println(errorLine);
        }

    }

    static void runMapper(String ip, int index) throws IOException {
        ProcessBuilder slave = new ProcessBuilder("ssh", "amponsem@" + ip, "cd /tmp/amponsem; java -jar mapper-1.0-SNAPSHOT.jar splits/S-" + index + ".txt ", "exit");

        Process p = slave.start();


        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));

        String line;

        while ((line = bufferedReader.readLine()) != null) {
            System.out.println("Output: " + line);
        }

        InputStream errorStream = p.getErrorStream();
        BufferedReader bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream));

        String errorLine;

        while ((errorLine = bufferedErrorReader.readLine()) != null) {
            System.out.println("Error: " + errorLine);
        }
        p.destroy();
    }

    static void copySplits(ArrayList<String> splits, int index, String ip) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("scp", splits.get(index), "amponsem@" + ip + ":/tmp/amponsem/splits");


        Process p = processBuilder.start();

        InputStream inputStream = p.getInputStream();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String line;

        while ((line = bufferedReader.readLine()) != null) {
            System.out.println("Output: " + line);
        }

        InputStream errorStream = p.getErrorStream();
        BufferedReader bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream));

        String errorLine;

        while ((errorLine = bufferedErrorReader.readLine()) != null) {
            System.out.println("Error: " + errorLine);
        }

        p.destroy();
    }

    static void makeSplitsDir(String ip) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "ls /tmp/amponsem/splits");

        Process p = processBuilder.start();

        BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String errorLine;
        while (errorStream.readLine() != null) {
            ProcessBuilder mkdirBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "mkdir -p /tmp/amponsem/splits");


            Process mkdirProcess = mkdirBuilder.start();

            BufferedReader mkdirErrorStream = new BufferedReader(new InputStreamReader(mkdirProcess.getErrorStream()));

            while ((errorLine = mkdirErrorStream.readLine()) != null) {
                System.out.println("Splits Error: " + errorLine);
            }
            mkdirProcess.destroy();
        }

        errorStream.close();
        p.destroy();
    }

    static void makeMapsDir(String ip) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "ls /tmp/amponsem/maps");


        Process p = processBuilder.start();

        BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String errorLine;
        while (errorStream.readLine() != null) {
            ProcessBuilder mkdirBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "mkdir -p /tmp/amponsem/maps");


            Process mkdirProcess = mkdirBuilder.start();

            BufferedReader mkdirErrorStream = new BufferedReader(new InputStreamReader(mkdirProcess.getErrorStream()));

            while ((errorLine = mkdirErrorStream.readLine()) != null) {
                System.out.println("Error: " + errorLine);
            }
            mkdirProcess.destroy();
        }

        errorStream.close();
        p.destroy();
    }

    static void makeShufflesDir(String ip) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "ls /tmp/amponsem/shuffles");


        Process p = processBuilder.start();

        BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String errorLine;
        while ((errorStream.readLine()) != null) {
            ProcessBuilder mkdirBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "mkdir -p /tmp/amponsem/shuffles");


            Process mkdirProcess = mkdirBuilder.start();

            BufferedReader mkdirErrorStream = new BufferedReader(new InputStreamReader(mkdirProcess.getErrorStream()));

            while ((errorLine = mkdirErrorStream.readLine()) != null) {
                System.out.println("Error: " + errorLine);
            }
            mkdirProcess.destroy();
        }

        errorStream.close();
        p.destroy();
        p.destroy();

    }

    static void makeShufflesReceivedDir(String ip) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "ls /tmp/amponsem/shufflesReceived");

        Process p = processBuilder.start();

        BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String errorLine;
        while ((errorStream.readLine()) != null) {
            ProcessBuilder mkdirBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "cd /tmp/amponsem; mkdir shufflesReceived");

            Process mkdirProcess = mkdirBuilder.start();

            BufferedReader mkdirErrorStream = new BufferedReader(new InputStreamReader(mkdirProcess.getErrorStream()));

            while ((errorLine = mkdirErrorStream.readLine()) != null) {
                System.out.println("Error: " + errorLine);
            }
            mkdirProcess.destroy();
        }

        p.destroy();
    }

    static void makeReducesDir(String ip) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "ls /tmp/amponsem/reduces");


        Process p = processBuilder.start();

        BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String errorLine;
        while ((errorStream.readLine()) != null) {
            ProcessBuilder mkdirBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "mkdir -p /tmp/amponsem/reduces");


            Process mkdirProcess = mkdirBuilder.start();

            BufferedReader mkdirErrorStream = new BufferedReader(new InputStreamReader(mkdirProcess.getErrorStream()));

            while ((errorLine = mkdirErrorStream.readLine()) != null) {
                System.out.println("Error: " + errorLine);
            }
        }

        errorStream.close();
        p.destroy();

    }

}
