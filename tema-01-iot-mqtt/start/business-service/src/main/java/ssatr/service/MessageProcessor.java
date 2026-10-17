package ssatr.service;

import java.util.concurrent.BlockingQueue;

/**
 * Preia mesajele de un anumit tip ("telemetry", "alarm" sau "status") din coada
 * inbox, le salvează și actualizează dashboard-ul.
 *
 * Formatul mesajelor (câmpuri separate prin ";"):
 *   telemetry:  deviceId;timestamp;temperatura
 *   alarm:      deviceId;timestamp;temperatura;prag
 *   status:     deviceId;timestamp;text
 */
public class MessageProcessor implements Runnable {

    private final String kind;
    private final BlockingQueue<String> inbox;
    private final CsvStorage storage;
    private final Dashboard dashboard;

    public MessageProcessor(String kind, BlockingQueue<String> inbox,
                            CsvStorage storage, Dashboard dashboard) {
        this.kind = kind;
        this.inbox = inbox;
        this.storage = storage;
        this.dashboard = dashboard;
    }

    @Override
    public void run() {
        // TODO 4:
        // - preluăm mesajul din coadă
        // - îl salvăm în CSV
        // - actualizăm dashboard-ul în funcție de tip
        // - afișăm imediat alarmele și confirmările
        // - raportăm mesajele invalide fără să oprim thread-ul

        while (!Thread.currentThread().isInterrupted()) {
            String message;

            try {
                message = inbox.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            // Salvăm mesajul exact cum a sosit.
            storage.append(kind, message);

            // Alarmele și confirmările se afișează imediat.
            if (kind.equals("alarm") || kind.equals("status")) {
                System.out.println(kind + ": " + message);
            }

            try {
                String[] fields = message.split(";", -1);

                if (fields[0].isBlank()) {
                    throw new IllegalArgumentException("lipsește deviceId");
                }

                switch (kind) {
                    case "telemetry" -> {
                        if (fields.length != 3) {
                            throw new IllegalArgumentException(
                                    "format telemetry invalid");
                        }

                        long timestamp = Long.parseLong(fields[1]);
                        double temperature = Double.parseDouble(fields[2]);

                        if (!Double.isFinite(temperature)) {
                            throw new IllegalArgumentException(
                                    "temperatura nu este finită");
                        }

                        dashboard.updateTelemetry(
                                fields[0], timestamp, temperature);
                    }

                    case "alarm" -> {
                        if (fields.length != 4) {
                            throw new IllegalArgumentException(
                                    "format alarm invalid");
                        }

                        long timestamp = Long.parseLong(fields[1]);
                        double temperature = Double.parseDouble(fields[2]);
                        double threshold = Double.parseDouble(fields[3]);

                        if (!Double.isFinite(temperature)
                                || !Double.isFinite(threshold)) {
                            throw new IllegalArgumentException(
                                    "temperatura sau pragul nu este finit");
                        }

                        dashboard.updateAlarm(
                                fields[0], timestamp, temperature, threshold);
                    }

                    case "status" -> {
                        if (fields.length != 3 || fields[2].isBlank()) {
                            throw new IllegalArgumentException(
                                    "format status invalid");
                        }

                        long timestamp = Long.parseLong(fields[1]);
                        dashboard.updateStatus(fields[0], timestamp);
                    }

                    default -> throw new IllegalArgumentException(
                            "tip de mesaj necunoscut: " + kind);
                }
            } catch (IllegalArgumentException e) {
                // NumberFormatException intră tot aici.
                System.out.println(
                        "Mesaj invalid (" + kind + "): "
                                + message + " — " + e.getMessage());
            }
        }
    }
}