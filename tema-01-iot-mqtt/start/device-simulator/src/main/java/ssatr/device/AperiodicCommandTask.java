package ssatr.device;

import java.util.concurrent.BlockingQueue;

/**
 * TASK APERIODIC: execută comenzile primite de la business service.
 * Comenzile sosesc oricând, pe MQTT, fără un interval minim între ele, și sunt
 * puse în coada commands. Fiecare comandă eliberează un job al taskului.
 * Comenzi care trebuie suportate:
 *   SET_INTERVAL <ms>       schimbă perioada T a taskului periodic
 *   SET_THRESHOLD <valoare> schimbă pragul de alarmă
 *   RESET                   readuce senzorul la valoarea inițială
 *   STATUS                  raportează perioada și pragul curente
 * După executarea fiecărei comenzi, device-ul publică pe Config.statusTopic(deviceId)
 * o confirmare: "deviceId;timestamp;text", de exemplu
 * "device-1;1727600000000;OK SET_INTERVAL 1000".
 */
public class AperiodicCommandTask implements Runnable {

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<String> commands;
    private final PeriodicTelemetryTask periodic;

    public AperiodicCommandTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                BlockingQueue<String> commands, PeriodicTelemetryTask periodic) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.commands = commands;
        this.periodic = periodic;
    }

    @Override
    public void run() {
        // TODO 3: implementați taskul aperiodic:
        //   - preluați comenzile din coadă: commands.take()
        //   - separați numele comenzii de parametru: command.split(" ")
        //   - executați comanda (periodic.setPeriodMs(...), sensor.reset() etc.)
        //   - publicați confirmarea pe Config.statusTopic(deviceId)
        //   - o comandă necunoscută sau un parametru invalid nu trebuie să
        //     oprească taskul: publicați "ERROR <motiv>"
        // Vezi exemplul exemple/01-taskuri/.../AperiodicTaskExample.java
        while (!Thread.currentThread().isInterrupted()) {
            String command;

            try {
                command = commands.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            String[] parts = command.trim().split("\\s+");
            String commandName = parts[0];
            String response;

            try {
                switch (commandName) {
                    case "SET_INTERVAL":
                        if (parts.length != 2) {
                            response = "ERROR SET_INTERVAL requires milliseconds";
                        } else {
                            long newPeriodMs = Long.parseLong(parts[1]);

                            if (newPeriodMs <= 0) {
                                response = "ERROR interval must be positive";
                            } else {
                                periodic.setPeriodMs(newPeriodMs);
                                response = "OK SET_INTERVAL " + newPeriodMs;
                            }
                        }
                        break;

                    case "SET_THRESHOLD":
                        if (parts.length != 2) {
                            response = "ERROR SET_THRESHOLD requires a value";
                        } else {
                            double newThreshold = Double.parseDouble(parts[1]);

                            if (!Double.isFinite(newThreshold)) {
                                response = "ERROR threshold must be a finite number";
                            } else {
                                periodic.setThreshold(newThreshold);
                                response = "OK SET_THRESHOLD " + newThreshold;
                            }
                        }
                        break;

                    case "RESET":
                        if (parts.length != 1) {
                            response = "ERROR RESET takes no parameters";
                        } else {
                            sensor.reset();
                            response = "OK RESET";
                        }
                        break;

                    case "STATUS":
                        if (parts.length != 1) {
                            response = "ERROR STATUS takes no parameters";
                        } else {
                            response = "OK STATUS periodMs="
                                    + periodic.getPeriodMs()
                                    + " threshold="
                                    + periodic.getThreshold();
                        }
                        break;

                    default:
                        response = "ERROR unknown command " + commandName;
                }
            } catch (NumberFormatException e) {
                response = "ERROR numeric parameter is invalid";
            }

            long timestamp = System.currentTimeMillis();
            String statusMessage = deviceId + ";" + timestamp + ";" + response;
            mqtt.publish(Config.statusTopic(deviceId), statusMessage);
        }
    }
}