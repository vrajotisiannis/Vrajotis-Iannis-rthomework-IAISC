// Device-ul IoT din tema 01, de data aceasta pe un ESP32-C3 Super Mini.
//
// Aceeași logică și același protocol ca device-simulator din tema 01, cu trei
// taskuri FreeRTOS în loc de trei thread-uri Java:
//
//   task periodic --(coada de alarme)--> task sporadic
//        |                                     |
//        +---- telemetry ---> MQTT <--- alarm -+
//                              |
//                          commands --> command_received() --(coada de comenzi)--> task aperiodic ---> status
//
// Gata implementate (nu trebuie modificate):
//   config.h         setările: Wi-Fi, broker, STUDENT, valorile inițiale; DEVICE_ID
//                    se citește din adresa MAC a plăcii
//   connection.h/cpp Wi-Fi, ora exactă (SNTP), clientul MQTT, mqtt_publish(), epoch_ms()
//
// Așa cum este, schița se conectează la Wi-Fi și la broker și afișează pe
// Serial comenzile primite. Taskurile le scrieți voi: TODO 0 ... TODO 3.
// Exemplele de pornire sunt în laboratorul 0 din rt-labs-esp32c3; vezi
// exemple/README.md din folderul temei.
//
// Placa: ESP32C3 Dev Module, USB CDC On Boot: Enabled. Serial Monitor: 115200 baud.

#include "config.h"
#include "connection.h"
#include <freertos/FreeRTOS.h>
#include <freertos/queue.h>
#include <freertos/task.h>
#include <errno.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

// Alarma păstrează temperatura și pragul valabile când a fost detectată.
struct AlarmEvent {
    float temperature;
    float threshold;
};

// Copiem textul comenzii în coadă; nu păstrăm pointerul primit de la MQTT.
struct CommandEvent {
    char text[COMMAND_MAX_LEN];
};

static QueueHandle_t alarmQueue = nullptr;
static QueueHandle_t commandQueue = nullptr;

static volatile uint32_t periodMs = PERIOD_MS_INITIAL;
static volatile float alarmThreshold = THRESHOLD_INITIAL;
static void periodicTask(void* parameter);
static void sporadicTask(void* parameter);
static void aperiodicTask(void* parameter);

// Temperatura cipului, în °C, de la senzorul intern al ESP32-C3.
// Este temperatura siliciului, nu a camerei: imediat după pornirea Wi-Fi-ului
// poate fi în jur de 50 °C, apoi scade în câteva minute spre 35-40 °C.
// Rezoluția este de aproximativ 1 °C. Ca să provocați alarme, puneți pragul
// cu SET_THRESHOLD cu un grad sub valoarea citită.
float read_temperature(void) {
    return temperatureRead();
}

// TODO 1: TASK PERIODIC, cu perioada T (inițial PERIOD_MS_INITIAL).
//   Fiecare job:
//   - citește temperatura: read_temperature()
//   - publică telemetria: mqtt_publish("telemetry", "deviceId;timestamp;temperatura")
//     (timestamp = epoch_ms(), tipărit cu %lld; temperatura cu "%.2f")
//   - dacă temperatura depășește pragul (inițial THRESHOLD_INITIAL), pune
//     un eveniment în coada de alarme, fără să se blocheze (timeout 0).
//     Taskul periodic NU tratează alarma: o tratează taskul sporadic.
//   Eliberările respectă grila r_k = r_0 + kT: folosiți așteptare absolută
//   (xTaskDelayUntil), nu relativă (delay, vTaskDelay), ca să nu apară derivă.
//   Perioada și pragul se schimbă prin comenzi (taskul aperiodic), deci sunt
//   variabile globale volatile, citite la fiecare job.
static void periodicTask(void* parameter) {
    (void)parameter;
    TickType_t lastRelease = xTaskGetTickCount();

    for (;;) {
        const uint32_t currentPeriod = periodMs;
        const float currentThreshold = alarmThreshold;
        const float temperature = read_temperature();

        char payload[96];
        snprintf(payload, sizeof(payload), "%s;%lld;%.2f",
                 DEVICE_ID,
                 (long long)epoch_ms(),
                 temperature);

        const bool published = mqtt_publish("telemetry", payload);
        Serial.printf("[periodic] temperatura=%.2f C, telemetrie %s\n",
                      temperature,
                      published ? "publicata" : "esec");

        if (temperature > currentThreshold) {
            const AlarmEvent event{temperature, currentThreshold};

            if (xQueueSend(alarmQueue, &event, 0) != pdPASS) {
                Serial.println("[periodic] coada de alarme plina; alarma pierduta");
            }
        }

        TickType_t periodTicks = pdMS_TO_TICKS(currentPeriod);
        if (periodTicks == 0) {
            periodTicks = 1;
        }

        xTaskDelayUntil(&lastRelease, periodTicks);
    }
}

// TODO 2: TASK SPORADIC, tratează alarmele de temperatură.
//   - așteaptă un eveniment din coada de alarme
//   - dacă de la ultima alarmă publicată au trecut mai puțin de
//     MIN_INTERARRIVAL_MS, respinge evenimentul (afișați un mesaj pe Serial)
//   - altfel publică: mqtt_publish("alarm", "deviceId;timestamp;temperatura;prag")
//   Pentru intervale folosiți un timp monoton, millis(), nu epoch_ms().
static void sporadicTask(void* parameter) {
    (void)parameter;
    bool hasPublishedAlarm = false;
    uint32_t lastAlarmMs = 0;

    for (;;) {
        AlarmEvent event;

        if (xQueueReceive(alarmQueue, &event, portMAX_DELAY) != pdPASS) {
            continue;
        }

        const uint32_t nowMs = millis();

        if (hasPublishedAlarm &&
            (uint32_t)(nowMs - lastAlarmMs) < MIN_INTERARRIVAL_MS) {
            Serial.println("[sporadic] alarma respinsa: a sosit prea curand");
            continue;
        }

        char payload[112];
        snprintf(payload, sizeof(payload), "%s;%lld;%.2f;%.2f",
                 DEVICE_ID,
                 (long long)epoch_ms(),
                 event.temperature,
                 event.threshold);

        const bool published = mqtt_publish("alarm", payload);

        if (published) {
            Serial.printf("[sporadic] alarma publicata: %.2f C, prag %.2f C\n",
                          event.temperature,
                          event.threshold);
            lastAlarmMs = nowMs;
            hasPublishedAlarm = true;
        } else {
            Serial.println("[sporadic] publicarea alarmei a esuat");
        }
    }
}
// TODO 3: TASK APERIODIC, execută comenzile primite de la business service.
//   Comenzile ajung în command_received() (mai jos), care le pune în coada de
//   comenzi; fiecare comandă eliberează un job al taskului:
//     SET_INTERVAL <ms>       schimbă perioada T a taskului periodic
//     SET_THRESHOLD <valoare> schimbă pragul de alarmă
//     RESET                   readuce perioada și pragul la valorile inițiale
//     STATUS                  raportează perioada și pragul curente
//   După fiecare comandă publică o confirmare:
//     mqtt_publish("status", "deviceId;timestamp;OK SET_INTERVAL 1000")
//   O comandă necunoscută sau un parametru invalid nu oprește taskul: se
//   publică "ERROR <motiv>".
static void publishStatus(const char* result) {
    char payload[COMMAND_MAX_LEN + 96];

    snprintf(payload, sizeof(payload), "%s;%lld;%s",
             DEVICE_ID, (long long)epoch_ms(), result);

    mqtt_publish("status", payload);
    Serial.printf("[command] %s\n", result);
}

static bool parsePeriod(const char* text, uint32_t* result) {
    if (text == nullptr || *text == '\0' || *text == '-') {
        return false;
    }

    errno = 0;
    char* end = nullptr;
    const unsigned long value = strtoul(text, &end, 10);

    if (errno == ERANGE || end == text || *end != '\0' ||
        value == 0 || value > UINT32_MAX) {
        return false;
    }

    const uint64_t ticks =
        ((uint64_t)value * configTICK_RATE_HZ) / 1000U;

    if (ticks == 0 || ticks > (uint64_t)portMAX_DELAY) {
        return false;
    }

    *result = (uint32_t)value;
    return true;
}

static void processCommand(const char* text) {
    char buffer[COMMAND_MAX_LEN];
    snprintf(buffer, sizeof(buffer), "%s", text);

    char* save = nullptr;
    char* name = strtok_r(buffer, " \t\r\n", &save);
    char* argument = strtok_r(nullptr, " \t\r\n", &save);
    char* extra = strtok_r(nullptr, " \t\r\n", &save);

    if (name == nullptr) {
        publishStatus("ERROR empty command");
        return;
    }

    if (strcmp(name, "SET_INTERVAL") == 0) {
        uint32_t value;

        if (argument == nullptr || extra != nullptr ||
            !parsePeriod(argument, &value)) {
            publishStatus("ERROR invalid SET_INTERVAL");
            return;
        }

        periodMs = value;

        char response[64];
        snprintf(response, sizeof(response), "OK SET_INTERVAL %lu",
                 (unsigned long)value);
        publishStatus(response);
        return;
    }

    if (strcmp(name, "SET_THRESHOLD") == 0) {
        if (argument == nullptr || extra != nullptr) {
            publishStatus("ERROR invalid SET_THRESHOLD");
            return;
        }

        errno = 0;
        char* end = nullptr;
        const float value = strtof(argument, &end);

        if (errno == ERANGE || end == argument || *end != '\0' ||
            !isfinite(value)) {
            publishStatus("ERROR invalid SET_THRESHOLD");
            return;
        }

        alarmThreshold = value;

        char response[64];
        snprintf(response, sizeof(response), "OK SET_THRESHOLD %.2f", value);
        publishStatus(response);
        return;
    }

    if (strcmp(name, "RESET") == 0) {
        if (argument != nullptr) {
            publishStatus("ERROR RESET takes no parameters");
            return;
        }

        periodMs = PERIOD_MS_INITIAL;
        alarmThreshold = THRESHOLD_INITIAL;
        publishStatus("OK RESET");
        return;
    }

    if (strcmp(name, "STATUS") == 0) {
        if (argument != nullptr) {
            publishStatus("ERROR STATUS takes no parameters");
            return;
        }

        char response[96];
        snprintf(response, sizeof(response),
                 "OK STATUS periodMs=%lu threshold=%.2f",
                 (unsigned long)periodMs, alarmThreshold);
        publishStatus(response);
        return;
    }

    char response[COMMAND_MAX_LEN + 32];
    snprintf(response, sizeof(response), "ERROR unknown command %.48s", name);
    publishStatus(response);
}

static void aperiodicTask(void* parameter) {
    (void)parameter;

    for (;;) {
        CommandEvent command;

        if (xQueueReceive(commandQueue, &command, portMAX_DELAY) == pdPASS) {
            processCommand(command.text);
        }
    }
}
// Apelată de clientul MQTT pentru fiecare comandă primită (vezi connection.h).
void command_received(const char* text) {
    Serial.printf("[mqtt] comandă primită: %s\n", text);
    // TODO 3: puneți comanda în coada de comenzi, fără să vă blocați
    //   (timeout 0). text este valid doar în timpul acestui apel: elementul
    //   cozii trebuie să conțină textul însuși (de exemplu un struct cu
    //   char text[COMMAND_MAX_LEN]), nu un pointer.
    CommandEvent command{};
    snprintf(command.text, sizeof(command.text), "%s", text);

    if (commandQueue == nullptr ||
        xQueueSend(commandQueue, &command, 0) != pdPASS) {
        Serial.println("[mqtt] coada de comenzi plina; comanda pierduta");
    }
}

void setup() {
    Serial.begin(115200);
    // Fără așteptare la scriere: cu placa la USB, dar fără Serial Monitor deschis,
    // fiecare Serial.printf() ar aștepta până la 2 s să fie citit și ar întârzia
    // taskul care scrie. Așa, textul pe care nu-l citește nimeni se pierde.
    Serial.setTxTimeoutMs(0);
    uint32_t t = millis();
    while (!Serial && millis() - t < 3000) delay(10);  // așteptăm Serial Monitor, cel mult 3 s
    delay(200);
    Serial.println();
    Serial.printf("=== tema 02: STUDENT %s, DEVICE_ID %s ===\n", STUDENT, DEVICE_ID);

    wifi_connect();
    time_sync();

    // TODO 0 (a): creați cele două cozi (xQueueCreate): coada de alarme și
    //   coada de comenzi. Înainte de mqtt_start(): de acolo încolo pot sosi
    //   comenzi, iar command_received() le pune în coada de comenzi.
    alarmQueue = xQueueCreate(10, sizeof(AlarmEvent));
    commandQueue = xQueueCreate(10, sizeof(CommandEvent));

    if (alarmQueue == nullptr || commandQueue == nullptr) {
    Serial.println("Eroare: nu s-au putut crea cozile FreeRTOS");
     for (;;) {
        delay(1000);
    }
}
    mqtt_start();

    // TODO 0 (b): porniți cele trei taskuri (xTaskCreate). După mqtt_start(),
    //   ca primele mesaje publicate să nu se piardă. Alegeți prioritățile
    //   între 2 și 19: peste loopTask (1), sub taskurile sistemului pentru
    //   evenimente și Wi-Fi (20, 23). Stiva, în octeți: cel puțin 4096.
    BaseType_t periodicStarted =
    xTaskCreate(periodicTask, "periodic", 4096, nullptr, 4, nullptr);
    BaseType_t sporadicStarted =
    xTaskCreate(sporadicTask, "sporadic", 4096, nullptr, 3, nullptr);
    BaseType_t aperiodicStarted =
    xTaskCreate(aperiodicTask, "aperiodic", 4096, nullptr, 3, nullptr);

    if (periodicStarted != pdPASS ||
    sporadicStarted != pdPASS ||
    aperiodicStarted != pdPASS) {
    Serial.println("Eroare: nu s-au putut porni toate taskurile FreeRTOS");
}
}

void loop() {
    // Totul se face în taskuri: loop() nu are nimic de făcut.
    delay(1000);
}
