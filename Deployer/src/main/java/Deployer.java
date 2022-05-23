import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Deployer {
    public static String username;
    public static String BUILD_PATH;
    public static String REMOTE_IPS;

    public static void main(String[] args) throws IOException {
        String machines = args[0];
        String inputFile = Paths.get(args[1]).toAbsolutePath().toString();
        String projectPath = args[2];
        BUILD_PATH = projectPath + "/build";
        REMOTE_IPS = BUILD_PATH + "/remoteips.txt";
        username = args[4];
        long ipsWithSplits = 0;

        ArrayList<String> workingIps = checkAvailableMachines(machines);
        /*
         * By using the list of working IP's we deploy using the **CommandRunner.java**
         * class to deploy the
         * various stages of map-reduce
         */
        if (!workingIps.isEmpty()) {
            ipsWithSplits = splitInputFile(inputFile, workingIps, projectPath);

            workingIps = workingIps.stream().limit(ipsWithSplits).collect(Collectors.toCollection(ArrayList::new));
            saveWorkingMachines(workingIps, projectPath);
            initializeEnvironment(workingIps);
            startCommandRunner(workingIps, projectPath);

            System.out.println("Compiling output...");
            compileOutput();

            System.exit(1);
        }
    }

    static int splitInputFile(String inputFile, ArrayList<String> workingIps, String projectPath) throws IOException {
        System.out.println("[SPLITTER] Splitting " + inputFile + " into (" + workingIps.size() + ")");
        LineIterator it = FileUtils.lineIterator(new File(inputFile), "UTF-8");
        ArrayList<File> splitFiles = new ArrayList<>();

        for (int i = 0; i < workingIps.size(); i++) {
            splitFiles.add(new File(BUILD_PATH + "/splits/S-" + i + ".txt"));
        }

        int index = 0;

        System.out.println("Has next: " + it.hasNext());
        try {
            while (it.hasNext()) {
                String line = it.nextLine();

                FileUtils.writeStringToFile(splitFiles.get(index % workingIps.size()), line + "\n",
                        StandardCharsets.UTF_8, true);
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

        Files.write(Paths.get(REMOTE_IPS), output.toString().getBytes(StandardCharsets.UTF_8));
    }

    static void compileOutput() throws IOException {
        File outputDir = new File(BUILD_PATH + "/output");
        StringBuilder output = new StringBuilder();

        if (!outputDir.isDirectory()) {
            System.out.println(outputDir.getAbsolutePath() + " is not a directory!");
            return;
        }
        for (File file : Objects.requireNonNull(outputDir.listFiles())) {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
                String line;

                while ((line = bufferedReader.readLine()) != null) {
                    System.out.println(line);
                    output.append(line).append("\n");
                }
            }
        }

        String outputFile = BUILD_PATH + "/output/OUTPUT.txt";
        Files.write(Paths.get(outputFile), output.toString().getBytes(StandardCharsets.UTF_8));

    }

    static boolean isNumeric(String s) {
        return Double.isNaN(Double.parseDouble(s));
    }

    static String getBaseDirectory(String subdir) {
        return String.format("/tmp/%s/%s", username, subdir);
    }

    static void startCommandRunner(ArrayList<String> workingIps, String projectPath)
            throws IOException {
        AtomicInteger index = new AtomicInteger(0);
        ArrayList<String> mapProcess = new ArrayList<>();
        ArrayList<String> shuffleProcess = new ArrayList<>();
        ArrayList<String> shufflesReceived = new ArrayList<>();
        ArrayList<Process> masterProcs = new ArrayList<>();
        int workingMachinesCount = workingIps.size();

        double total = 0.0;

        for (String ip : workingIps) {
            ProcessBuilder master = new ProcessBuilder("java", "-Xms2048m", "-Xmx2048m", "-jar",
                    BUILD_PATH + "/jars/command-runner-1.0-SNAPSHOT.jar", ip,
                    String.valueOf(index.get()), REMOTE_IPS, projectPath, username);

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
                System.out.println("Master processes: " + masterProcs.size());

                while (scanner.hasNextLine()) {
                    results = scanner.nextLine();
                    System.out.println("Results: " + results);
                    if (results.contains("TOTAL")) {
                        String _results = results;
                        String[] totals = _results.split(" ");
                        total += Double.parseDouble(totals[2]);
                    }
                    System.out.println("Output: " + results);
                    if (results.contains(getBaseDirectory("maps"))) {
                        mapProcess.add(results);
                    }
                    if (results.contains(getBaseDirectory("shuffles"))) {
                        shuffleProcess.add(results);
                    }
                    if (results.contains(getBaseDirectory("shufflesReceived/"))) {
                        shufflesReceived.add(results);
                    }
                }

                try (Scanner pScanner = new Scanner(p.getErrorStream());) {
                    while (pScanner.hasNextLine()) {
                        results = pScanner.nextLine();
                        System.out.println("[ERROR] " + results);
                    }

                    p.destroy();
                } catch (Exception e) {
                    e.printStackTrace();
                }

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

        try (BufferedReader successStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
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
                setupFilesPath + "mapper-1.0-SNAPSHOT.jar",
                setupFilesPath + "shuffler-1.0-SNAPSHOT.jar",
                setupFilesPath + "reducer-1.0-SNAPSHOT.jar",
                username + "@" + ip + ":" + getBaseDirectory(""));

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
            System.out.println(future.get(10, TimeUnit.SECONDS)); // timeout is in 10 seconds
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            System.err.println("Timeout");
        }

    }

    static void makeRootDirectory(String ip) throws IOException {
        ProcessBuilder mkdirProcessBuilder = new ProcessBuilder("ssh", "-o ConnectTimeout=2", username + "@" + ip,
                "mkdir -p " + getBaseDirectory(""));

        Process mkdirProcess = setupProcess(mkdirProcessBuilder);

        mkdirProcess.destroyForcibly();
    }

    static void initializeEnvironment(ArrayList<String> workingIps) {
        System.out.println("Initializing environment...");
        workingIps.forEach(ip -> {
            System.out.println("IP: " + ip);
            ProcessBuilder processBuilder = new ProcessBuilder("ssh", "-o ConnectTimeout=2", username + "@" + ip,
                    "ls " + getBaseDirectory(""));
            System.out.println(Arrays.toString(processBuilder.command().toArray()));
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

                copySlaveProgram(ip, BUILD_PATH + "/jars/");

                p.destroyForcibly();
            } catch (IOException e) {
                e.printStackTrace();
            }

        });

        System.out.println("Slave copied successfully!");
    }
}
