package ssatr.service;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
/**
 * Ultima stare cunoscută a fiecărui device: ultima temperatură, momentul
 * ultimului mesaj și numărul de alarme.
 * Este actualizată de thread-urile MessageProcessor și citită de CommandConsole,
 * deci accesul trebuie sincronizat.
 */
public class Dashboard {

    // TODO 5: alegeți structura de date pentru starea device-urilor
    //   (de exemplu un Map<String, ...> cu cheia deviceId)
    //   și adăugați metodele de actualizare apelate din MessageProcessor.

    private final Map<String, DeviceState> devices = new HashMap<>();

    private static class DeviceState {
        double temperature = Double.NaN;
        long timestamp;
        double threshold;
        int alarmCount;
    }

    private DeviceState stateFor(String deviceId) {
        return devices.computeIfAbsent(deviceId, id -> new DeviceState());
    }

    public synchronized void updateTelemetry(
            String deviceId, long timestamp, double temperature) {
        DeviceState state = stateFor(deviceId);
        state.timestamp = timestamp;
        state.temperature = temperature;
    }

    public synchronized void updateAlarm(
            String deviceId, long timestamp,
            double temperature, double threshold) {
        DeviceState state = stateFor(deviceId);
        state.timestamp = timestamp;
        state.temperature = temperature;
        state.threshold = threshold;
        state.alarmCount++;
    }

    public synchronized void updateStatus(String deviceId, long timestamp) {
        stateFor(deviceId).timestamp = timestamp;
    }

    /** Afișează un tabel cu starea tuturor device-urilor. */
    public synchronized void print() {
        System.out.printf("%-16s %-10s %-18s %s%n",
                "DEVICE", "TEMP", "ULTIMUL MESAJ", "ALARME");

        if (devices.isEmpty()) {
            System.out.println("(nu există încă mesaje de la device-uri)");
            return;
        }

        long now = System.currentTimeMillis();

        for (Map.Entry<String, DeviceState> entry : devices.entrySet()) {
            DeviceState state = entry.getValue();

            String temperature = Double.isNaN(state.temperature)
                    ? "--"
                    : String.format(Locale.US, "%.2f", state.temperature);

            String age = state.timestamp <= 0
                    ? "--"
                    : formatAge(Math.max(0, now - state.timestamp));

            System.out.printf("%-16s %-10s %-18s %d%n",
                    entry.getKey(), temperature, age, state.alarmCount);
        }
    }

    private String formatAge(long ageMs) {
        long seconds = ageMs / 1000;

        if (seconds < 60) {
            return "acum " + seconds + " s";
        }
        long minutes = seconds / 60;
        if (minutes < 60) {
            return "acum " + minutes + " min";
        }
        long hours = minutes / 60;
        return "acum " + hours + " h";
    }
}