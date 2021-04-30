import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class
Deployer {
    public static void main(String[] args) throws IOException {
        String machines = args[0];
        String inputFile = Paths.get(args[1]).toAbsolutePath().toString();
        String projectPath = args[2];
        String setupFilesPath = args[3];
        long ipsWithSplits = 0;

        ArrayList<String> workingIps = checkAvailableMachines(machines);

        if (!workingIps.isEmpty()) {
            ipsWithSplits = splitInputFile(inputFile, workingIps, projectPath);

            System.out.println(ipsWithSplits);
        }

        workingIps = workingIps.stream().limit(ipsWithSplits).collect(Collectors.toCollection(ArrayList::new));
        saveWorkingMachines(workingIps, projectPath);
        initializeEnvironment(workingIps, setupFilesPath);
        startCommandRunner(workingIps, projectPath, setupFilesPath);

        compileOutput(projectPath);

        System.exit(1);
    }

    static int splitInputFile(String inputFile, ArrayList<String> workingIps, String projectPath) throws IOException {
        System.out.println("[SPLITTER] Splitting " + inputFile + " into (" + workingIps.size() + ")");
        LineIterator it = FileUtils.lineIterator(new File(inputFile), "UTF-8");
        ArrayList<File> splitFiles = new ArrayList<>();

        for (int i = 0; i < workingIps.size(); i++) {
            splitFiles.add(new File(projectPath + "/splits/S-" + i + ".txt"));
        }

        int index = 0;
        try {
            while (it.hasNext()) {
                String line = it.nextLine();

                FileUtils.writeStringToFile(splitFiles.get(index % workingIps.size()), line + "\n", StandardCharsets.UTF_8, true);
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return index;
    }

    static void saveWorkingMachines(ArrayList<String> workingMachines, String projectPath) throws IOException {
        StringBuilder output = new StringBuilder();
        for (String machine : workingMachines) {
            output.append(machine).append("\n");
        }

        Files.write(Paths.get(projectPath + "/machines.txt"), output.toString().getBytes(StandardCharsets.UTF_8));
    }

    static void compileOutput(String projectPath) throws IOException {
        File outputDir = new File(projectPath + "/output");
        StringBuilder output = new StringBuilder();

        for (File file : Objects.requireNonNull(outputDir.listFiles())) {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        String outputFile = projectPath + "/output/OUTPUT.txt";
        Files.write(Paths.get(outputFile), output.toString().getBytes(StandardCharsets.UTF_8));

    }

    static boolean isNumeric(String s) {
        return Double.isNaN(Double.parseDouble(s));
    }

    static void startCommandRunner(ArrayList<String> workingIps, String projectPath, String setupFilesPath) throws IOException {
        AtomicInteger index = new AtomicInteger(0);
        ArrayList<String> mapProcess = new ArrayList<>();
        ArrayList<String> shuffleProcess = new ArrayList<>();
        ArrayList<String> shufflesReceived = new ArrayList<>();
        ArrayList<Process> masterProcs = new ArrayList<>();
        int workingMachinesCount = workingIps.size();

        double total = 0.0;

        for (String ip : workingIps) {
            ProcessBuilder master = new ProcessBuilder("/usr/lib/jvm/default-java/bin/java", "-Xms2048m", "-Xmx2048m", "-jar",
                    setupFilesPath + "/CommandRunner/target/CommandRunner-1.0-SNAPSHOT.jar", ip, String.valueOf(index.get()), projectPath + "/machines.txt", projectPath);

            System.out.println("Starting program on [" + ip + "] [index " + index + "] : ");

            Process p = master.start();

            masterProcs.add(p);
            index.addAndGet(1);

        }

        for (Process p : masterProcs) {
            try {


                boolean timeout = p.waitFor(50, TimeUnit.MINUTES);

                if (!timeout) {
                    System.out.println("There is a timeout");
                    p.destroyForcibly();
                }

                Scanner scanner = new Scanner(p.getInputStream());
                String results;

                while (scanner.hasNextLine()) {
                    results = scanner.nextLine();
                    if (results.contains("TOTAL")) {
                        String _results = results;
                        String[] totals = _results.split(" ");
                        total += Double.parseDouble(totals[2]);
//                        System.out.println(Arrays.toString(totals));
//                        Arrays.stream(totals).filter(s->!isNumeric(s))
                    }
                    System.out.println("Output: " + results);
                    if (results.contains("/tmp/amponsem/maps/")) {
                        mapProcess.add(results);
                    }
                    if (results.contains("/tmp/amponsem/shuffles/")) {
                        shuffleProcess.add(results);
                    }
                    if (results.contains("/tmp/amponsem/shufflesReceived/")) {
                        shufflesReceived.add(results);
                    }
                }

                scanner = new Scanner(p.getErrorStream());
                while (scanner.hasNextLine()) {
                    results = scanner.nextLine();
                    System.out.println("[ERROR] " + results);
                }


                p.destroy();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
        if (workingMachinesCount > 0) {
            if (mapProcess.size() == workingMachinesCount) {
                System.out.println("MAP FINISHED");
            }
            if (shuffleProcess.size() >= workingMachinesCount) {
                System.out.println("SHUFFLE FINISHED");
            }
            if (shufflesReceived.size() >= workingMachinesCount) {
                System.out.println("SHUFFLES RECEIVED FINISHED");
            }

        }
        System.out.println("[PROGRAM-TOTAL] " + total + " ms");
        System.out.println("Ending program...");
    }

    static public String ping(Process p, String ip) throws IOException {
        return getString(p, ip);
    }

    private static String getString(Process p, String ip) {
        boolean worked = false;

        Scanner success = new Scanner(p.getInputStream());

        if (success.hasNextLine()) {
            worked = true;
        }

        success.close();

        p.destroy();
        return worked ? ip : null;
    }

    static ArrayList<String> checkAvailableMachines(String machines) throws IOException {
        Scanner scanner = new Scanner(new FileInputStream(machines));
//        ArrayList<String> workingIps = new ArrayList<>();
        ArrayList<String> ips = new ArrayList<>();
        HashMap<Process, String> processes = new HashMap<>();

        while (scanner.hasNextLine()) {
            ips.add(scanner.nextLine());
        }

        scanner.close();

        for (String ip : ips) {
            System.out.println("[PING] " + ip);
            ProcessBuilder processBuilder = new ProcessBuilder("ping", "-c 1", ip);
            Process p = processBuilder.start();
            processes.put(p, ip);
        }

        ips.clear();
        processes.forEach((p, ip) -> {
            try {
                ips.add(ping(p, ip));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return ips;
    }

    static Process setupProcess(ProcessBuilder processBuilder) throws IOException {
        Process p = processBuilder.start();

        String scpError;

        try (BufferedReader successStream = new BufferedReader(new InputStreamReader(p.getErrorStream())); BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            while ((scpError = successStream.readLine()) != null) {
                System.out.println("Output: " + scpError);
            }

            while ((scpError = errorStream.readLine()) != null) {
                System.out.println("Error: " + scpError);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return p;
    }

    static void copySlaveProgram(String ip, String setupFilesPath) {
        ProcessBuilder scpProcessBuilder = new ProcessBuilder("scp",
                setupFilesPath + "/Mapper/target/mapper-1.0-SNAPSHOT.jar",
                setupFilesPath + "/Shuffler/target/shuffler-1.0-SNAPSHOT.jar",
                setupFilesPath + "/Reducer/target/reducer-1.0-SNAPSHOT.jar",
                "amponsem@" + ip + ":/tmp/amponsem");


        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(() -> {
            try {
                Process scpProcess = setupProcess(scpProcessBuilder);
                scpProcess.destroyForcibly();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return "OK";
        });

        try {
            System.out.println(future.get(10, TimeUnit.SECONDS)); //timeout is in 10 seconds
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            System.err.println("Timeout");
        }

    }

    static void makeRootDirectory(String ip) throws IOException {
        ProcessBuilder mkdirProcessBuilder = new ProcessBuilder("ssh", "-o ConnectTimeout=2", "amponsem@" + ip,
                "mkdir -p /tmp/amponsem/");

        Process mkdirProcess = setupProcess(mkdirProcessBuilder);

        mkdirProcess.destroyForcibly();
    }

    static void initializeEnvironment(ArrayList<String> workingIps, String setupFilesPath) {
        workingIps.forEach(ip -> {

            System.out.println("IP: " + ip);
            ProcessBuilder processBuilder = new ProcessBuilder("ssh", "-o ConnectTimeout=2", "amponsem@" + ip, "ls /tmp/amponsem");


            try {
                Process p = processBuilder.start();

                String line;

                try (BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                     BufferedReader successStream = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    while ((line = successStream.readLine()) != null) {
                        System.out.println("Output: " + line);
                    }
                    while (errorStream.readLine() != null) {
                        makeRootDirectory(ip);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                copySlaveProgram(ip, setupFilesPath);

                p.destroyForcibly();
            } catch (IOException e) {
                e.printStackTrace();
            }

        });

        System.out.println("Slave copied successfully!");
    }
}
