import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.*;

public class Cleaner {
    public static void main(String[] args) throws IOException {
        String machines = args[0];
        HashSet<String> workingIps = checkAvailableMachines(machines);
        cleanRemoteFolder(workingIps);
        System.exit(1);
    }

    static void cleanRemoteFolder(HashSet<String> workingIps) throws IOException {
        ArrayList<Process> processes = new ArrayList<>();
        ArrayList<String> messages = new ArrayList<>();

        workingIps.forEach(ip -> {
                    ProcessBuilder processBuilder = new ProcessBuilder("ssh", "amponsem@" + ip, "rm -rf /tmp/amponsem");

                    Process p = null;
                    try {
                        p = processBuilder.start();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                    processes.add(p);

                }
        );

        for (Process p : processes) {

            String message;
            BufferedReader errorBuffer = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            BufferedReader successBuffer = new BufferedReader(new InputStreamReader(p.getInputStream()));

            if (successBuffer.readLine() != null) {
                messages.add("[CLEANER] done!");
            }
            successBuffer.close();
            while ((message = errorBuffer.readLine()) != null) {
                System.out.println("Error: " + message);
            }
            errorBuffer.close();

            p.destroy();


        }
        if (messages.size() == workingIps.size()) {
            System.out.println("[CLEANER] Done for ("+workingIps.size()+") machines!");
        }
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
                String finalIp = ip;
                Future<String> future = executor.submit(() -> {

                    processBuilder.command("ping", finalIp);
                    Process p = processBuilder.start();

                    BufferedReader commandOuput = new BufferedReader(new InputStreamReader(p.getInputStream()));


                    if (commandOuput.readLine() != null) {
                        ProcessBuilder sshConnection = new ProcessBuilder("ssh", "-o StrictHostKeyChecking=no", "amponsem@" + finalIp);
                        Process sshProcess = sshConnection.start();

                        BufferedReader sshBufferedReader = new BufferedReader(new InputStreamReader(sshProcess.getInputStream()));
                        String sshLine;
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

                        sshProcess.destroy();
                    }
                    commandOuput.close();

                    InputStream errorStream = p.getErrorStream();
                    BufferedReader bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream));

                    String errorLine;

                    while ((errorLine = bufferedErrorReader.readLine()) != null) {
                        System.out.println("Error: " + errorLine);
                    }
                    errorStream.close();


                    p.destroy();
                    return "OK";
                });
                try {
                    System.out.println(future.get(2, TimeUnit.SECONDS)); //timeout is in 2 seconds

                } catch (TimeoutException ignored) {
                }
                executor.shutdownNow();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return workingIps;
    }
}
