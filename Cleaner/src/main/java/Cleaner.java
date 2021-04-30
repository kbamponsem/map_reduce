import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Cleaner {
    public static void main(String[] args) throws IOException {
        String machines = args[0];
        HashSet<String> workingIps = checkAvailableMachines(machines);
        cleanRemoteFolder(workingIps);
        System.exit(1);
    }

    static void cleanRemoteFolder(HashSet<String> workingIps) throws IOException {
        System.out.println("Cleaning remote folders...");
        ArrayList<Process> processes = new ArrayList<>();
        boolean noError = true;

        System.out.println(Arrays.toString(workingIps.toArray()));
        for (String ip : workingIps) {
            if (ip != null) {
                ProcessBuilder processBuilder = new ProcessBuilder("ssh","-o ConnectTimeout=2", "amponsem@" + ip, "rm -rf /tmp/amponsem");
                Process p;
                try {
                    p = processBuilder.start();
                    processes.add(p);
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }

        }

        for (Process p : processes) {

            String message;
            Scanner scanner = new Scanner(new InputStreamReader(p.getErrorStream()));

            while (scanner.hasNextLine()) {
                message = scanner.nextLine();
                System.out.println(message);
                noError = false;
            }
            scanner.close();

            p.destroy();
        }
        if (noError) {
            System.out.println("[CLEANER] Done for (" + workingIps.stream().filter(Objects::nonNull).count() + ") machines!");
        }
    }

    static public String cleaner(Process p, String ip) throws IOException {
        if (ip != null) {
            return getString(p, ip);
        }
        return null;
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

    static HashSet<String> checkAvailableMachines(String machines) throws IOException {
        Scanner scanner = new Scanner(new FileInputStream(machines));
        HashSet<String> workingIps = new HashSet<>();
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

        processes.clear();

        for (String ip : ips) {
            if (ip != null) {
                System.out.println("[CLEANER] " + ip);
                ProcessBuilder sshConnection = new ProcessBuilder("ssh", "-o ConnectTimeout=2", "amponsem@" + ip);
                Process sshProcess = sshConnection.start();

                processes.put(sshProcess, ip);
            }
        }

        processes.forEach((p, ip) -> {
            try {
                workingIps.add(cleaner(p, ip));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return workingIps;
    }
}
