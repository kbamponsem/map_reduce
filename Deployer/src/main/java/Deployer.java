import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class
Deployer {
    public static void main(String[] args) throws IOException {
        String machines = args[0];
        String inputFile = Paths.get(args[1]).toAbsolutePath().toString();
        String projectPath = args[2];
        String setupFilesPath = args[3];

        HashSet<String> workingIps = checkAvailableMachines(machines);
        saveWorkingMachines(workingIps, projectPath);

        if (!workingIps.isEmpty()) {
            splitInputFile(inputFile, workingIps, projectPath);
        }

        initializeEnvironment(workingIps, setupFilesPath);
        startCommandRunner(workingIps, projectPath, setupFilesPath);

        compileOutput(projectPath);

        System.exit(1);
    }

    static void splitInputFile(String inputFile, HashSet<String> workingIps, String projectPath) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
        ArrayList<String> lines = new ArrayList<>();

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            if (!line.isEmpty() || line.equals(""))
                lines.add(line);
        }

        int workingMachinesCount = workingIps.size();
        int counter = lines.size() / workingMachinesCount;
        int remainder = lines.size() % workingMachinesCount;

        for (int i = 0; i < workingMachinesCount; i++) {
            int start = i * counter;
            int finish = (i * counter) + counter;

            if (remainder > 0) {
                finish += 1;
                remainder--;
            }
            List<String> split = lines.subList(start, finish);
            StringBuilder output = new StringBuilder();
            split.forEach(s -> output.append(s).append("\n"));
            Files.write(Paths.get(projectPath+"/splits/S-" + i + ".txt"), output.toString().getBytes(StandardCharsets.UTF_8));
        }

    }

    static void saveWorkingMachines(HashSet<String> workingMachines, String projectPath) throws IOException {
        StringBuilder output = new StringBuilder();
        for (String machine : workingMachines) {
            output.append(machine).append("\n");
        }

        Files.write(Paths.get(projectPath+"/machines.txt"), output.toString().getBytes(StandardCharsets.UTF_8));
    }

    static void compileOutput(String projectPath) throws IOException {
        File outputDir = new File(projectPath+"/output");
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

    static void startCommandRunner(HashSet<String> workingIps, String projectPath, String setupFilesPath) throws IOException {
        AtomicInteger index = new AtomicInteger(0);
        ArrayList<String> mapProcess = new ArrayList<>();
        ArrayList<String> shuffleProcess = new ArrayList<>();
        ArrayList<String> shufflesReceived = new ArrayList<>();
        ArrayList<Process> masterProcs = new ArrayList<>();
        int workingMachinesCount = workingIps.size();


        for (String ip : workingIps) {
            ProcessBuilder master = new ProcessBuilder("/usr/lib/jvm/default-java/bin/java", "-jar",
                    setupFilesPath+"CommandRunner/target/CommandRunner-1.0-SNAPSHOT.jar", ip, String.valueOf(index.get()), projectPath+"/machines.txt", projectPath);

            System.out.println("Starting program on [" + ip + "] [index " + index + "] : ");

            Process p = master.start();

            masterProcs.add(p);
            index.addAndGet(1);

        }

        for (Process p : masterProcs) {
            try {


                boolean timeout = p.waitFor(50, TimeUnit.SECONDS);

                if (!timeout) {
                    System.out.println("There is a timeout");
                    p.destroyForcibly();
                }

                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                String results;

                while ((results = bufferedReader.readLine()) != null) {
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

                while ((results = errorReader.readLine()) != null) {
                    System.out.println("Error: " + results);
                }

                p.destroy();

            } catch (IOException | InterruptedException e) {
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
        System.out.println("Ending program...");
    }

    static HashSet<String> checkAvailableMachines(String machines) throws IOException {
        FileInputStream remoteIps = new FileInputStream(machines);
        BufferedReader bufferedReader;
        ProcessBuilder processBuilder = new ProcessBuilder();

        HashSet<String> workingIps = new HashSet<>();

        try {
            bufferedReader = new BufferedReader(new InputStreamReader(remoteIps));
            String ip;

            while ((ip = bufferedReader.readLine()) != null) {
                ExecutorService executor = Executors.newSingleThreadExecutor();
                String finalIp = ip.trim();
                if (!finalIp.equals("") && finalIp.matches("^(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])(\\.(?!$)|$)){4}$")) {
                    Future<String> future = executor.submit(() -> {
                        processBuilder.command("ping", "-c", "2", finalIp);
                        Process p = processBuilder.start();

                        BufferedReader commandOuput = new BufferedReader(new InputStreamReader(p.getInputStream()));


                        if (commandOuput.readLine() != null) {
                            ProcessBuilder sshConnection = new ProcessBuilder("ssh", "-o StrictHostKeyChecking=no", "amponsem@" + finalIp);

                            Process sshProcess = sshConnection.start();

                            BufferedReader sshBufferedReader = new BufferedReader(new InputStreamReader(sshProcess.getInputStream()));
                            while (sshBufferedReader.readLine() != null) {
                                workingIps.add(finalIp);
                            }
                            sshBufferedReader.close();

                            InputStream errorStream = sshProcess.getErrorStream();
                            BufferedReader bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream));

                            String errorLine;

                            while ((errorLine = bufferedErrorReader.readLine()) != null) {
                                System.out.println("Error: " + errorLine);
                            }
                            sshBufferedReader.close();
                            sshProcess.destroyForcibly();

                        }

                        InputStream errorStream = processBuilder.start().getErrorStream();
                        BufferedReader bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream));

                        String errorLine;

                        while ((errorLine = bufferedErrorReader.readLine()) != null) {
                            System.out.println("Error: " + errorLine);
                        }
                        p.destroyForcibly();
                        return "OK";
                    });
                    try {
                        System.out.println(future.get(2, TimeUnit.SECONDS)); //timeout is in 2 seconds
                    } catch (TimeoutException ignored) {

                    }
                    executor.shutdownNow();

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return workingIps;
    }

    static void copySlaveProgram(String ip, String setupFilesPath) {
        ProcessBuilder scpProcessBuilder = new ProcessBuilder("scp",
                setupFilesPath+"/Mapper/target/mapper-1.0-SNAPSHOT.jar",
                setupFilesPath+"/Shuffler/target/shuffler-1.0-SNAPSHOT.jar",
                setupFilesPath+"/Reducer/target/reducer-1.0-SNAPSHOT.jar",
                "amponsem@" + ip + ":/tmp/amponsem");


        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(() -> {
            try {
                Process scpProcess = scpProcessBuilder.start();

                BufferedReader scpErrorStream = new BufferedReader(new InputStreamReader(scpProcess.getErrorStream()));
                BufferedReader scpSuccessStream = new BufferedReader(new InputStreamReader(scpProcess.getInputStream()));
                String scpError;

                while ((scpError = scpSuccessStream.readLine()) != null) {
                    System.out.println("Output: " + scpError);
                }

                while ((scpError = scpErrorStream.readLine()) != null) {
                    System.out.println("Error: " + scpError);
                }
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
        ProcessBuilder mkdirProcessBuilder = new ProcessBuilder("ssh", "-o StrictHostKeyChecking=no", "amponsem@" + ip,
                "mkdir -p /tmp/amponsem/");


        Process mkdirProcess = mkdirProcessBuilder.start();

        BufferedReader successStream = new BufferedReader(new InputStreamReader(mkdirProcess.getInputStream()));
        String successLine;
        while ((successLine = successStream.readLine()) != null) {
            System.out.println("Output: " + successLine);
        }
        successStream.close();
        mkdirProcess.destroyForcibly();
    }

    static void initializeEnvironment(HashSet<String> workingIps, String setupFilesPath) {
        workingIps.forEach(ip -> {

            System.out.println("IP: " + ip);
            ProcessBuilder processBuilder = new ProcessBuilder("ssh", "-o StrictHostKeyChecking=no", "amponsem@" + ip, "ls /tmp/amponsem");


            try {
                Process p = processBuilder.start();

                BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                BufferedReader successStream = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line;

                while ((line = successStream.readLine()) != null) {
                    System.out.println("Output: " + line);
                }

                while (errorStream.readLine() != null) {
                    makeRootDirectory(ip);
                }
                errorStream.close();

                copySlaveProgram(ip, setupFilesPath);

                p.destroyForcibly();
            } catch (IOException e) {
                e.printStackTrace();
            }

        });

        System.out.println("Slave copied successfully!");
    }
}
