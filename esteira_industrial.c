// Esteira Industrial (ESP32 + FreeRTOS) — 6 tasks com touch ISR
// - ENC_SENSE (periódica 5 ms) -> notifica SPD_CTRL
// - SPD_CTRL (hard RT) -> controle PI simulado, trata HMI (soft) se solicitado
// - SORT_ACT (hard RT, evento Touch B) -> aciona "desviador"
// - SAFETY_TASK (hard RT, evento Touch D) -> E-stop
// Compilado com ESP-IDF 5.x

#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/event_groups.h"
#include <stdatomic.h>
#include "esp_timer.h"
#include "esp_log.h"
#include "driver/gpio.h"
#include "driver/touch_sens.h"

//novos includes:
#include "esp_wifi.h"
#include "esp_event.h"
#include "nvs_flash.h"
#include "lwip/sockets.h"
#include "lwip/netdb.h"
#include "esp_sntp.h"
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>

#define TAG "ESTEIRA"

//VARIAVEIS PARA A TASK TIME
static const char *TAG_TIME = "TIME_TASK";

#define TIME_PERIOD_MS       1000
#define TIME_DEADLINE_US     1000000
#define TIME_STACK           3072
#define TIME_PRIORITY        1

static portMUX_TYPE time_mux = portMUX_INITIALIZER_UNLOCKED;

static int64_t last_sntp_sync_us = -1;
static uint32_t sntp_sync_generation = 0;

static uint32_t time_total = 0;
static uint32_t time_deadline_misses = 0;
static int64_t time_c_max_us = 0;

// 0 = UDP; 1 = TCP. Um protocolo por ensaio.
#define MONITOR_USE_PROTOCOLO 1

// Mapeamento para usar wi-fi
static const char *TAG_WIFI = "TCP_SERVER";
#define WIFI_SSID "S25U_Rafa"
#define WIFI_PASS "umacoisaae"
#define TCP_PORT 7000 // qualquer número entre 0 e 65535

#define PC_IP   "10.245.178.176"   // IP do PC - use ifconfig (Linux) ou ipconfig (Windows) para achar o IP
#define PC_PORT 7000 // qualquer número entre 0 e 65535

// ====== Mapeamento dos touch pads ======
#define TP_OBJ   TOUCH_PAD_NUM7   // Touch B -> detecção de objeto (desvio)
#define TP_HMI   TOUCH_PAD_NUM8   // Touch C -> HMI/telemetria
#define TP_ESTOP TOUCH_PAD_NUM9   // Touch D -> E-stop

// ====== Periodicidade, prioridades, stack ======
#define ENC_T_MS        5
#define PRIO_ESTOP      5 // RM: 3 DM: 5 CS: 5
#define PRIO_ENC        4 // RM: 5 DM: 4 CS: 4
#define PRIO_CTRL       3 // RM: 4 DM: 3 CS: 3
#define PRIO_SORT       3 // RM: 3 DM: 2 CS: 3
#define PRIO_MON        1 // prioridade monitor
#define STK             3072

// ====== Deadlines (do enunciado) ======
#define DEADLINE_ENC_US       5000    // 5 ms
#define DEADLINE_CTRL_US     10000    // 10 ms
#define DEADLINE_SORT_US     10000    // 10 ms
#define DEADLINE_SAFETY_US    5000    // 5 ms
#define DEADLINE_MONITOR_US 500000    // 500 ms
#define MONITOR_PERIOD_MS      500

// comandos dentro do monitor
typedef enum {
  CMD_SORT,
  CMD_ESTOP,
  CMD_STATUS,
  CMD_PING
}monitor_cmd_t;
typedef struct {
  monitor_cmd_t cmd;
  int64_t event_us; // Recepcao na ESP32, antes da espera pelo monitor.
} monitor_request_t;

// tipo para mensagem do monitor
typedef struct {
  char payload[256];
}monitor_msg_t;

// PARA MONITOR
static QueueHandle_t qMonitorTx = NULL;  //fila para mandar os log
static QueueHandle_t qMonitorCmd = NULL; //fila de comando

static TaskHandle_t hMonitor = NULL;
static TaskHandle_t hNet = NULL;

// Estatísticas temporais da MONITOR_TASK
static uint32_t monitor_total = 0;
static uint32_t monitor_misses = 0;
static int64_t monitor_c_max_us = 0;
static int64_t monitor_r_max_us = 0;

// ====== Handles/IPC ======
static TaskHandle_t hENC = NULL, hCTRL = NULL, hSORT = NULL, hSAFE = NULL;

// Notificação ENC->CTRL (direct-to-task notification)
static TaskHandle_t hCtrlNotify = NULL;

// Fila de eventos para o SORT_ACT
typedef struct {
    int64_t t_evt_us;   // timestamp do toque (para logging/latência)
} sort_evt_t;
static QueueHandle_t qSort = NULL;

// Semáforos para E-stop e HMI
static SemaphoreHandle_t semHMI   = NULL;

// Estado "simulado" da esteira
typedef struct {
  float rpm;
  float pos_mm;
  float set_rpm;
}belt_state_t;

typedef struct {
  uint32_t total, misses;
  int64_t wcrt_us, measured_max_us, latency_max_us, jitter_max_us, last_start;
} task_metrics_t;

typedef struct{
  task_metrics_t task[6]; // ENC, SPD, SORT, SAFETY, MONITOR, TIME
  int misses_enc;
  int total_enc;
  int misses_spd;
  int total_spd;
  int misses_sort;
  int total_sort;
  int misses_stop;
  int total_stop;
}misses_t;

misses_t misses = {0};

static belt_state_t g_belt = { .rpm = 0.f, .pos_mm = 0.f, .set_rpm = 120.0f };

// variaveis para conseguir o tempo ao tocar e fazer algo
// A fila de seguranca preserva um timestamp por acionamento (touch ou rede).

//registra um instante
// Instrumentacao: timestamps monotonicos em us; hora civil apenas na rede.
// Nenhuma verificacao original de deadline e substituida.
typedef enum { MET_ENC, MET_SPD, MET_SORT, MET_SAFE, MET_MON, MET_TIME, MET_COUNT } metric_id_t;
typedef struct {
    uint32_t seq, task_no;
    int id;
    int64_t event, start, end, measured;
    bool miss;
} execution_t;
static QueueHandle_t qExecutions;
static QueueHandle_t qSafetyTimes;
static QueueHandle_t qCtrlTimes;
static portMUX_TYPE metrics_mux = portMUX_INITIALIZER_UNLOCKED;
static uint32_t execution_seq, log_drops, input_drops;
static _Atomic uint32_t tx_errors, truncated, state_drops;

static void input_drop(bool from_isr)
{
    if (from_isr) portENTER_CRITICAL_ISR(&metrics_mux);
    else portENTER_CRITICAL(&metrics_mux);
    input_drops++;
    if (from_isr) portEXIT_CRITICAL_ISR(&metrics_mux);
    else portEXIT_CRITICAL(&metrics_mux);
}

// Referencia periodica com resolucao de tick, sem alterar o teste de deadline.
// Desconta os ticks de atraso em relacao ao instante esperado por DelayUntil.
static int64_t periodic_event(TickType_t expected, int64_t start)
{
    int32_t late = (int32_t)(xTaskGetTickCount()-expected);
    return start - (late > 0 ? (int64_t)late*1000000/configTICK_RATE_HZ : 0);
}

static void record_execution(metric_id_t id, int64_t event, int64_t start,
                             int64_t end, int64_t measured, int64_t deadline,
                             int64_t period)
{
    execution_t e = {.id=id, .event=event, .start=start, .end=end,
                     .measured=measured, .miss=measured > deadline};
    portENTER_CRITICAL(&metrics_mux);
    e.seq = execution_seq++;
    task_metrics_t *m = &misses.task[id];
    m->total++;
    e.task_no = m->total;
    m->misses += e.miss;
    int64_t response = end - event, latency = start - event;
    if (response > m->wcrt_us) m->wcrt_us = response;
    if (measured > m->measured_max_us) m->measured_max_us = measured;
    if (latency > m->latency_max_us) m->latency_max_us = latency;
    if (period && m->last_start) {
        int64_t jitter = llabs(start - m->last_start - period);
        if (jitter > m->jitter_max_us) m->jitter_max_us = jitter;
    }
    m->last_start = start;
    portEXIT_CRITICAL(&metrics_mux);
    // Nunca bloquear uma tarefa critica por causa do coletor.
    if (xQueueSend(qExecutions, &e, 0) != pdTRUE) {
        portENTER_CRITICAL(&metrics_mux);
        log_drops++;
        portEXIT_CRITICAL(&metrics_mux);
    }
}

static bool next_network_message(monitor_msg_t *msg)
{
    // Alterna estado/resumos e execucoes para evitar starvation da telemetria.
    static bool prefer_state;
    prefer_state = !prefer_state;
    if (prefer_state && xQueueReceive(qMonitorTx, msg, 0) == pdTRUE) return true;
    execution_t e;
    if (xQueueReceive(qExecutions, &e, 0) == pdTRUE) {
        int n = snprintf(msg->payload, sizeof(msg->payload),
            "{\"type\":\"exec\",\"seq\":%" PRIu32 ",\"task\":%d,\"n\":%" PRIu32 ",\"e\":%" PRId64
            ",\"s\":%" PRId64 ",\"f\":%" PRId64 ",\"r\":%" PRId64 ",\"miss\":%d}",
            e.seq, e.id, e.task_no, e.event, e.start, e.end, e.measured, e.miss);
        if (n > 0 && n < sizeof(msg->payload)) return true;
        truncated++;
    }
    return xQueueReceive(qMonitorTx, msg, 0) == pdTRUE;
}

// Adiciona hora de envio, estado SNTP e protocolo a cada mensagem, mantendo 256 bytes.
static bool stamp_message(monitor_msg_t *msg)
{
    size_t used = strlen(msg->payload);
    if (!used || msg->payload[used-1] != '}') return false;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    portENTER_CRITICAL(&time_mux);
    bool synced = sntp_sync_generation != 0;
    portEXIT_CRITICAL(&time_mux);
    int n = snprintf(msg->payload + used-1, sizeof(msg->payload)-used+1,
        ",\"tx_ms\":%" PRId64 ",\"sync\":%d,\"p\":\"%s\"}",
        (int64_t)tv.tv_sec*1000 + tv.tv_usec/1000, synced, MONITOR_USE_PROTOCOLO ? "TCP" : "UDP");
    if (n < 0 || (size_t)n >= sizeof(msg->payload)-used+1) {
        truncated++;
        return false;
    }
    return true;
}

static bool make_pong(monitor_msg_t *msg, const char *ping)
{
    char echo[33];
    size_t i;
    for (i=0; i<sizeof(echo)-1 && ping[i]; ++i) {
        unsigned char c = ping[i];
        echo[i] = (c < 32 || c == '"' || c == '\\') ? '_' : c;
    }
    echo[i] = 0;
    snprintf(msg->payload, sizeof(msg->payload),
             "{\"type\":\"PONG\",\"echo\":\"%s\"}", echo);
    return stamp_message(msg);
}

#if MONITOR_USE_PROTOCOLO
static bool send_all(int sock, const char *data, size_t size)
{
    while (size) {
        int n = send(sock, data, size, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        data += n;
        size -= n;
    }
    return true;
}

#endif

static void queue_state(const monitor_msg_t *msg)
{
    if (xQueueSend(qMonitorTx, msg, 0) != pdTRUE) state_drops++;
}

static void publish_summary(void)
{
    static unsigned id;
    task_metrics_t m;
    uint32_t dropped, inputs;
    portENTER_CRITICAL(&metrics_mux);
    m = misses.task[id];
    dropped = log_drops;
    inputs = input_drops;
    portEXIT_CRITICAL(&metrics_mux);
    monitor_msg_t msg;
    int n = snprintf(msg.payload, sizeof(msg.payload),
        "{\"type\":\"stats\",\"task\":%u,\"n\":%" PRIu32 ",\"misses\":%" PRIu32
        ",\"wcrt\":%" PRId64 ",\"lat\":%" PRId64 ",\"jit\":%" PRId64 "}",
        id, m.total, m.misses, m.wcrt_us, m.latency_max_us, m.jitter_max_us);
    if (n > 0 && n < sizeof(msg.payload)) queue_state(&msg);
    if (id == 0) {
        snprintf(msg.payload, sizeof(msg.payload),
            "{\"type\":\"config\",\"cpu_mhz\":%d,\"cores\":%d,\"tick_hz\":%d,"
            "\"preemption\":%d,\"slicing\":%d,\"priorities\":[%d,%d,%d,%d,%d,%d]}",
            CONFIG_ESP_DEFAULT_CPU_FREQ_MHZ, configNUMBER_OF_CORES, configTICK_RATE_HZ,
            configUSE_PREEMPTION, configUSE_TIME_SLICING,
            PRIO_ENC, PRIO_CTRL, PRIO_SORT, PRIO_ESTOP, PRIO_MON, TIME_PRIORITY);
        queue_state(&msg);
    }
    id = (id+1) % MET_COUNT;
    snprintf(msg.payload, sizeof(msg.payload),
        "{\"type\":\"health\",\"log_drops\":%" PRIu32 ",\"input_drops\":%" PRIu32
        ",\"tx_errors\":%" PRIu32 ",\"truncated\":%" PRIu32 ",\"state_drops\":%" PRIu32 "}",
        dropped, inputs, (uint32_t)tx_errors, (uint32_t)truncated, (uint32_t)state_drops);
    queue_state(&msg);
}

static void publish_cpu(void)
{
#if configUSE_TRACE_FACILITY && configGENERATE_RUN_TIME_STATS
    static TaskStatus_t tasks[48];
    static configRUN_TIME_COUNTER_TYPE last_idle[configNUMBER_OF_CORES], last_total;
    static bool initialized;
    configRUN_TIME_COUNTER_TYPE total;
    UBaseType_t count = uxTaskGetSystemState(tasks, 48, &total);
    if (!count) return;
    uint64_t idle_delta = 0;
    unsigned idle_count = 0;
    for (UBaseType_t i=0; i<count; ++i) {
        for (unsigned core=0; core<configNUMBER_OF_CORES; ++core) {
            if (tasks[i].xHandle == xTaskGetIdleTaskHandleForCore(core)) {
                idle_delta += (configRUN_TIME_COUNTER_TYPE)(tasks[i].ulRunTimeCounter-last_idle[core]);
                last_idle[core] = tasks[i].ulRunTimeCounter;
                idle_count++;
            }
        }
    }
    configRUN_TIME_COUNTER_TYPE elapsed = total-last_total;
    if (initialized && elapsed && idle_count == configNUMBER_OF_CORES) {
        double busy = 100.0*(1.0-(double)idle_delta/((double)elapsed*configNUMBER_OF_CORES));
        if (busy < 0) busy = 0;
        if (busy > 100) busy = 100;
        monitor_msg_t msg;
        snprintf(msg.payload, sizeof(msg.payload),
                 "{\"type\":\"cpu\",\"busy_pct\":%.2f,\"cores\":%d}", busy, configNUMBER_OF_CORES);
        queue_state(&msg);
    }
    last_total = total;
    initialized = true;
#endif
}

static void time_sync_notification_cb(struct timeval *tv)
{
  (void)tv;

  int64_t now_us = esp_timer_get_time();

  portENTER_CRITICAL(&time_mux);
  last_sntp_sync_us = now_us;
  sntp_sync_generation++;
  portEXIT_CRITICAL(&time_mux);

  ESP_LOGI(TAG_TIME, "Tempo sincronizado via SNTP");
}

static void time_sntp_init(void){
    // Fuso horário de Brasília: UTC-3
    setenv("TZ", "BRT3", 1);
    tzset();

    // Callback chamado quando o relógio é sincronizado
    esp_sntp_set_time_sync_notification_cb(
      time_sync_notification_cb
    );

    // Servidores NTP
    esp_sntp_setservername(0, "a.st1.ntp.br");
    esp_sntp_setservername(1, "b.st1.ntp.br");
    esp_sntp_setservername(2, "pool.ntp.org");

    // Configura e inicia o SNTP
    esp_sntp_setoperatingmode(ESP_SNTP_OPMODE_POLL);
    esp_sntp_init();

    ESP_LOGI(TAG_TIME, "SNTP iniciado; aguardando sincronizacao");
}

static void time_task(void *arg)
{
    (void)arg;

    TickType_t next = xTaskGetTickCount();
    const TickType_t period = pdMS_TO_TICKS(TIME_PERIOD_MS);

    uint32_t last_generation = 0;
    uint32_t cycles_since_sync = 0;

    for (;;) {
        int64_t t_start = esp_timer_get_time();
        int64_t time_event = periodic_event(next, t_start);

        // Captura hora atual do sistema.
        struct timeval tv;
        gettimeofday(&tv, NULL);

        struct tm timeinfo = {0};
        localtime_r(&tv.tv_sec, &timeinfo);

        char timestamp[40];

        strftime(timestamp, sizeof(timestamp),
                 "%Y-%m-%d %H:%M:%S", &timeinfo);

        // Lê o estado da última sincronização SNTP.
        int64_t sync_us;
        uint32_t generation;

        portENTER_CRITICAL(&time_mux);
        sync_us = last_sntp_sync_us;
        generation = sntp_sync_generation;
        portEXIT_CRITICAL(&time_mux);

        bool synced = (generation > 0);

        if (generation != last_generation) {
            last_generation = generation;
            cycles_since_sync = 0;
        } else if (synced) {
            cycles_since_sync++;
        }

        // Milissegundos da hora civil.
        int milliseconds = (int)(tv.tv_usec / 1000);

        // Medição do tempo de execução C.
        int64_t t_end = esp_timer_get_time();
        int64_t C_us = t_end - t_start;

        time_total++;

        if (C_us > time_c_max_us) {
            time_c_max_us = C_us;
        }

        if (C_us > TIME_DEADLINE_US) {
            time_deadline_misses++;

            ESP_LOGW(TAG_TIME,
                     "Deadline excedido: C=%" PRId64 " us",
                     C_us);
        }

        // Tempo monotônico desde a última sincronização.
        int64_t sync_age_us = -1;

        if (synced) {
            sync_age_us = t_end - sync_us;
        }

        ESP_LOGI(TAG_TIME,
                 "hora=%s.%03d synced=%s "
                 "ciclos_desde_sync=%" PRIu32 " "
                 "sync_age_ms=%" PRId64 " "
                 "C_us=%" PRId64,
                 timestamp,
                 milliseconds,
                 synced ? "sim" : "nao",
                 cycles_since_sync,
                 sync_age_us >= 0 ? sync_age_us / 1000 : -1,
                 C_us);

        record_execution(MET_TIME, time_event, t_start, t_end, C_us,
                         TIME_DEADLINE_US, TIME_PERIOD_MS*1000LL);
        monitor_msg_t sync_msg;
        snprintf(sync_msg.payload, sizeof(sync_msg.payload),
            "{\"type\":\"time\",\"cycles\":%" PRIu32 ",\"age_ms\":%" PRId64 "}",
            cycles_since_sync, sync_age_us >= 0 ? sync_age_us/1000 : -1);
        queue_state(&sync_msg);
        vTaskDelayUntil(&next, period);
    }
}

static void monitor_process_command(monitor_request_t request)
{
  switch (request.cmd) {
  case CMD_SORT: {
    sort_evt_t ev = {
      .t_evt_us = request.event_us
    };

    if (xQueueSend(qSort, &ev, 0) != pdTRUE) {
      input_drop(false);
      ESP_LOGW(TAG_WIFI, "MONITOR: fila SORT cheia");
    }
    break;
  }
  case CMD_ESTOP:
    // Mesmo mecanismo de sinalização usado pela tarefa de segurança.
    int64_t remote_event = request.event_us;
    if (xQueueSend(qSafetyTimes, &remote_event, 0) != pdTRUE) input_drop(false);
    break;
  case CMD_STATUS:
    // O estado será enviado na próxima mensagem periódica.
    break;
  case CMD_PING:
    // O tratamento da resposta PING/RTT fica na tarefa de rede.
    break;
  }
}

static void monitor_task(void *arg)
{
  TickType_t next = xTaskGetTickCount();
  const TickType_t period = pdMS_TO_TICKS(MONITOR_PERIOD_MS);

  monitor_total = 0;
  monitor_misses = 0;
  monitor_c_max_us = 0;
  monitor_r_max_us = 0;

  for (;;) {
    // Início da ativação: usado para medir resposta e execução.
    const int64_t t_release = esp_timer_get_time();
    const int64_t t_start = esp_timer_get_time();
    const int64_t monitor_event = periodic_event(next, t_start);

    // 1. Processa os comandos pendentes sem bloquear.
    monitor_request_t request;

    while (xQueueReceive(qMonitorCmd, &request, 0) == pdTRUE) {
      monitor_process_command(request);
    }

    // 2. Coleta o estado e prepara a telemetria.
    monitor_msg_t msg;

    static uint32_t seq = 0;
    int n = snprintf(msg.payload, sizeof(msg.payload),
      "{\"type\":\"telemetry\",\"seq\":%" PRIu32 ",\"rpm\":%.2f,\"pos_mm\":%.2f,\"set_rpm\":%.2f}",
      seq++, (double)g_belt.rpm, (double)g_belt.pos_mm, (double)g_belt.set_rpm);

    // Enfileira sem bloquear a tarefa de monitoramento.
    if (n > 0 && n < sizeof(msg.payload)) queue_state(&msg);
    else truncated++;

    // 4. Mede C e verifica o deadline.
    const int64_t t_end = esp_timer_get_time();
    const int64_t C_us = t_end - t_start;
    const int64_t R_us = t_end - t_release;

    monitor_total++;

    if (C_us > monitor_c_max_us) {
      monitor_c_max_us = C_us;
    }

    if (R_us > monitor_r_max_us) {
      monitor_r_max_us = R_us;
    }

    if (R_us > DEADLINE_MONITOR_US) {
      monitor_misses++;
      ESP_LOGW(TAG_WIFI, "MONITOR DEADLINE: R=%lld us, C=%lld us", (long long)R_us, (long long)C_us);
    }

    record_execution(MET_MON, monitor_event, t_start, t_end, R_us,
                     DEADLINE_MONITOR_US, MONITOR_PERIOD_MS*1000LL);
    publish_summary();
    // 5. Espera até a próxima ativação periódica.
    vTaskDelayUntil(&next, period);
  }
}

#if !MONITOR_USE_PROTOCOLO
static void udp_task(void *arg){
  int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);

  if (sock < 0) {
    ESP_LOGE(TAG_WIFI, "Falha ao criar socket UDP");
    vTaskDelete(NULL);
    return;
  }

  struct sockaddr_in local = {0};
  local.sin_family = AF_INET;
  local.sin_port = htons(PC_PORT);
  local.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(sock, (struct sockaddr *)&local, sizeof(local)) < 0) {
    ESP_LOGE(TAG_WIFI, "Falha no bind UDP");
    close(sock);
    vTaskDelete(NULL);
    return;
  }

  struct timeval timeout = {
    .tv_sec = 0,
    .tv_usec = 1000
  };
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  struct sockaddr_in pc = {0};
  pc.sin_family = AF_INET;
  pc.sin_port = htons(PC_PORT);
  inet_pton(AF_INET, PC_IP, &pc.sin_addr.s_addr);

  char rx[256];

  for (;;) {
    // Envia a telemetria pendente sem bloquear esperando a fila.
    monitor_msg_t msg;
    for (int batch=0; batch<32 && next_network_message(&msg); ++batch) {
      if (stamp_message(&msg) && sendto(sock, msg.payload, strlen(msg.payload), 0,
          (struct sockaddr *)&pc, sizeof(pc)) < 0) tx_errors++;
    }

    static int64_t last_cpu;
    if (esp_timer_get_time()-last_cpu >= 1000000) {
      publish_cpu();
      last_cpu = esp_timer_get_time();
    }
    // Recebe comandos. O timeout limita o tempo bloqueado.
    struct sockaddr_in source;
    socklen_t source_len = sizeof(source);

    int len = recvfrom(sock, rx, sizeof(rx) - 1, 0, (struct sockaddr *)&source, &source_len);

    if (len > 0) {
      rx[len] = '\0';

      monitor_cmd_t cmd = CMD_STATUS;
      bool valid = true;

      if (strcmp(rx, "SORT") == 0) {
        cmd = CMD_SORT;
      }else if (strcmp(rx, "ESTOP") == 0) {
        cmd = CMD_ESTOP;
      }else if (strcmp(rx, "STATUS") == 0) {
        cmd = CMD_STATUS;
      }else if (strncmp(rx, "PING", 4) == 0) {
        // PING é tratado separadamente para preservar o ID.
        monitor_msg_t reply;
        if (make_pong(&reply, rx) && sendto(sock, reply.payload, strlen(reply.payload), 0,
            (struct sockaddr *)&source, source_len) < 0) tx_errors++;
        valid = false;
      }else {
        valid = false;
        ESP_LOGW(TAG_WIFI, "Comando UDP desconhecido: %s", rx);
      }

      monitor_request_t request = {.cmd=cmd, .event_us=esp_timer_get_time()};
      if (valid && xQueueSend(qMonitorCmd, &request, 0) != pdTRUE) {
        input_drop(false);
        ESP_LOGW(TAG_WIFI, "Fila de comandos cheia");
      }
    }
  }
}

#endif

static EventGroupHandle_t wifi_events;
#define WIFI_READY BIT0
static void wifi_event(void *arg, esp_event_base_t base, int32_t id, void *data)
{
  if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
    xEventGroupSetBits(wifi_events, WIFI_READY);
    ESP_LOGI(TAG_WIFI, "WiFi com IP; iniciando experimento");
  } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
    xEventGroupClearBits(wifi_events, WIFI_READY);
    esp_wifi_connect();
  }
}

//Configura WiFi da ESP32
static void wifi_init(void) {
  ESP_ERROR_CHECK(esp_netif_init());
  ESP_ERROR_CHECK(esp_event_loop_create_default());
  esp_netif_create_default_wifi_sta();
  wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
  ESP_ERROR_CHECK(esp_wifi_init(&cfg));
  wifi_config_t wifi_config = {
    .sta = {
      .threshold.authmode = WIFI_AUTH_WPA2_PSK,
    },
  };
  strcpy((char*)wifi_config.sta.ssid, WIFI_SSID);
  strcpy((char*)wifi_config.sta.password, WIFI_PASS);
  ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
  ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
  wifi_events = xEventGroupCreate();
  if (!wifi_events) abort();
  ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, wifi_event, NULL));
  ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, WIFI_EVENT_STA_DISCONNECTED, wifi_event, NULL));
  ESP_ERROR_CHECK(esp_wifi_start());
  ESP_LOGI(TAG_WIFI, "Conectando ao WiFi...");
  ESP_ERROR_CHECK(esp_wifi_connect());
  xEventGroupWaitBits(wifi_events, WIFI_READY, pdFALSE, pdTRUE, portMAX_DELAY);
}

#if MONITOR_USE_PROTOCOLO
static void tcp_task(void *arg){
  int listen_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);

  if (listen_fd < 0) {
    ESP_LOGE(TAG_WIFI, "Falha ao criar socket TCP");
    vTaskDelete(NULL);
    return;
  }

  int reuse = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  struct sockaddr_in addr = {0};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(TCP_PORT);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
    listen(listen_fd, 1) < 0) {
    ESP_LOGE(TAG_WIFI, "Falha ao iniciar servidor TCP");
    close(listen_fd);
    vTaskDelete(NULL);
    return;
  }

  ESP_LOGI(TAG_WIFI, "Servidor TCP ativo na porta %d", TCP_PORT);

  for (;;) {
    struct sockaddr_in source;
    socklen_t source_len = sizeof(source);
    int client = accept(listen_fd, (struct sockaddr *)&source, &source_len);

    if (client < 0) {
      continue;
    }

    struct timeval timeout = {
      .tv_sec = 0,
      .tv_usec = 1000
    };

    setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    ESP_LOGI(TAG_WIFI, "Cliente TCP conectado");

    const char *hello = "{\"type\":\"connected\"}\n";
    if (!send_all(client, hello, strlen(hello))) {
      tx_errors++;
      close(client);
      continue;
    }
    char rx[256];
    char command[128];
    size_t command_used = 0;
    bool command_overflow = false;
    for (;;) {
      // Transmite mensagens pendentes.
      monitor_msg_t msg;

      for (int batch=0; batch<32 && next_network_message(&msg); ++batch) {
        if (!stamp_message(&msg)) continue;
        char line[300];

        int n = snprintf(line, sizeof(line), "%s\n", msg.payload);
        if (n <= 0 || n >= sizeof(line) || !send_all(client, line, n)) {
          tx_errors++;
          goto tcp_disconnected;
        }
      }

      static int64_t last_cpu;
      if (esp_timer_get_time()-last_cpu >= 1000000) {
        publish_cpu();
        last_cpu = esp_timer_get_time();
      }
      // Recebe comandos sem espera indefinida.
      int len = recv(client, rx, sizeof(rx) - 1, 0);
      if (len > 0) {
        rx[len] = '\0';

        // Acumula fragmentos TCP e separa comandos por newline.
        for (int i=0; i<len; ++i) {
          if (rx[i] == '\r') continue;
          if (rx[i] != '\n') {
            if (command_used < sizeof(command)-1 && !command_overflow)
              command[command_used++] = rx[i];
            else command_overflow = true;
            continue;
          }
          if (command_overflow) {
            command_overflow = false;
            command_used = 0;
            input_drop(false);
            continue;
          }
          command[command_used] = '\0';
          command_used = 0;
          monitor_cmd_t cmd = CMD_STATUS;
          bool valid = true;

          if (strcmp(command, "SORT") == 0) {
            cmd = CMD_SORT;
          }else if (strcmp(command, "ESTOP") == 0) {
            cmd = CMD_ESTOP;
          }else if (strcmp(command, "STATUS") == 0) {
            cmd = CMD_STATUS;
          }else if (strncmp(command, "PING", 4) == 0) {
            monitor_msg_t reply;
            char line[300];
            if (make_pong(&reply, command)) {
              int size = snprintf(line, sizeof(line), "%s\n", reply.payload);
              if (!send_all(client, line, size)) {
                tx_errors++;
                goto tcp_disconnected;
              }
            }

            valid = false;
          }else {
            valid = false;
            ESP_LOGW(TAG_WIFI, "Comando TCP desconhecido: %s", command);
          }

          monitor_request_t request = {.cmd=cmd, .event_us=esp_timer_get_time()};
          if (valid && xQueueSend(qMonitorCmd, &request, 0) != pdTRUE) {
            input_drop(false);
            ESP_LOGW(TAG_WIFI, "Fila de comandos cheia");
          }
        } // proximo comando recebido
      }else if (len == 0) {
        // O PC encerrou a conexão.
        break;
      }else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        // Erro real de recepção.
        break;
      }

      vTaskDelay(pdMS_TO_TICKS(1));
    }

tcp_disconnected:
    shutdown(client, SHUT_RDWR);
    close(client);
    ESP_LOGI(TAG_WIFI, "Cliente TCP desconectado");
  }
}

// ====== Util: busy loop previsível (~WCET) ======
#endif

static inline void cpu_tight_loop_us(uint32_t us)
{
  int64_t start = esp_timer_get_time();
  while ((esp_timer_get_time() - start) < us) {
    __asm__ __volatile__("nop");
  }
}

// ====== ENC_SENSE (periódica 5 ms): estima velocidade/posição ======
static void task_enc_sense(void *arg)
{
  TickType_t next = xTaskGetTickCount();
  const TickType_t T = pdMS_TO_TICKS(ENC_T_MS);
  if (!T) abort();

  misses.misses_enc = 0;
  misses.total_enc = 0;
  int ciclos = 0;

  for (;;) {
    misses.total_enc++;
    int64_t t0 = esp_timer_get_time();
    int64_t enc_event = periodic_event(next, t0);
    // Dinâmica simulada: aproxima rpm do setpoint, integra posição
    float err = g_belt.set_rpm - g_belt.rpm;
    g_belt.rpm += 0.05f * err;             // aproximação lenta para simular inércia
    g_belt.pos_mm += (g_belt.rpm / 60.0f) * (ENC_T_MS / 1000.0f) * 100.0f; // 100 mm por rev (exemplo)

    // Carga determinística ~0.6 ms
    cpu_tight_loop_us(600);

    // Notifica controle
    if (hCtrlNotify){
      int64_t notified_at = esp_timer_get_time();
      if (xQueueSend(qCtrlTimes, &notified_at, 0) != pdTRUE) input_drop(false);
      else xTaskNotifyGive(hCtrlNotify);
    }

    ciclos++;
    if(ciclos > 2000){
      ESP_LOGI(TAG, "Enc_MISS=%d Spd_MISS=%d Sort_MISS=%d Stop_MISS=%d", misses.misses_enc, misses.misses_spd, misses.misses_sort, misses.misses_stop);
      ESP_LOGI(TAG, "Enc_TOTAL=%d Spd_TOTAL=%d Sort_TOTAL=%d Stop_TOTAL=%d", misses.total_enc, misses.total_spd, misses.total_sort, misses.total_stop);
      ESP_LOGI(TAG, "Enc_PCT=%.2f Spd_PCT=%.2f Sort_PCT=%.2f Stop_PCT=%.2f",
          ((float)misses.misses_enc / misses.total_enc) * 100.0f,
          ((float)misses.misses_spd / misses.total_spd) * 100.0f,
          ((float)misses.misses_sort / (misses.total_sort ? misses.total_sort : 1)) * 100.0f,
          ((float)misses.misses_stop / (misses.total_stop ? misses.total_stop : 1)) * 100.0f
      );
      ciclos = 0;
    }

    int64_t total_enc = esp_timer_get_time() - t0;

    if(total_enc > DEADLINE_ENC_US){
      ESP_LOGI(TAG, "ENC_SENSE DEADLINE! total=%lld microsegundos (us)", total_enc);
      misses.misses_enc++;
    }else{
      //ESP_LOGI(TAG, "total=%lld us", total_enc);
    }

    record_execution(MET_ENC, enc_event, t0, t0+total_enc, total_enc, DEADLINE_ENC_US, ENC_T_MS*1000LL);
    vTaskDelayUntil(&next, T);
  }
}

// ====== SPD_CTRL (encadeada): controle PI simulado + HMI (soft) ======
static void task_spd_ctrl(void *arg)
{
  // um controlador PI minimalista e simulado
  float kp = 0.4f, ki = 0.1f, integ = 0.f;

  misses.misses_spd = 0;
  misses.total_spd = 0;

  for (;;) {

    ulTaskNotifyTake(pdFALSE, portMAX_DELAY);
    int64_t notified_at;
    if (xQueueReceive(qCtrlTimes, &notified_at, 0) != pdTRUE) {
      input_drop(false);
      notified_at = esp_timer_get_time();
    }
    misses.total_spd++;

    int64_t t0 = esp_timer_get_time();

    // Controle (hard): alvo é g_belt.set_rpm, atuando em g_belt.rpm (simulado)
    float err = g_belt.set_rpm - g_belt.rpm;
    integ += err * (ENC_T_MS / 1000.0f);
    float u = kp * err + ki * integ;
    // "Aplicar" u: aqui apenas ajustamos o setpoint ligeiramente (mock)
    g_belt.set_rpm += 0.1f * u;

    // Carga determinística ~1.2 ms
    cpu_tight_loop_us(1200);

    // Trecho não-crítico (soft): se HMI solicitada, imprime e retorna rápido
    if (xSemaphoreTake(semHMI, 0) == pdTRUE) {
      ESP_LOGI(TAG, "HMI: rpm=%.1f set=%.1f pos=%.1fmm",g_belt.rpm, g_belt.set_rpm, g_belt.pos_mm);
      cpu_tight_loop_us(400); // ~0.4 ms (soft)
    }

    int64_t total_spd = esp_timer_get_time() - t0;

    if(total_spd > DEADLINE_CTRL_US){
      ESP_LOGI(TAG, "HMI/Telemetria DEADLINE! total=%lld microsegundos (us)", total_spd);
      misses.misses_spd++;
    }else{
      //ESP_LOGI(TAG, "SPD CTRL total=%lld", total_spd);
    }
    record_execution(MET_SPD, notified_at, t0, t0+total_spd, total_spd,
                     DEADLINE_CTRL_US, 0);
  }
}

// ====== SORT_ACT (evento Touch B): aciona "desviador" no tempo certo ======
static void task_sort_act(void *arg)
{
  sort_evt_t ev;
  misses.misses_sort = 0;
  misses.total_sort = 0;
  for (;;) {
    if (xQueueReceive(qSort, &ev, portMAX_DELAY) == pdTRUE) {
      misses.total_sort++;
      int64_t t0 = esp_timer_get_time();
      int64_t tempo_touch = t0 - ev.t_evt_us; //é o tempo que levou para tocar e entrar na função

      // Preparação de janela crítica (ex.: 0.7 ms)
      cpu_tight_loop_us(700);

      // Aqui você acionaria GPIO/solenóide no "instante"
      // Nesta demo, apenas logamos o tempo gasto:
      int64_t dt = esp_timer_get_time() - t0;

      int64_t total_b = dt + tempo_touch;

      ESP_LOGI(TAG, "SORT_ACT: desviador acionado, exec=%lld microsegundos (us), toque=%lld microssegundos(us), total=%lld microssegundos (us)", dt, tempo_touch, total_b);

      if(total_b > DEADLINE_SORT_US){
        ESP_LOGI(TAG, "Sort_Act DEADLINE! total=%lld microssegundos (us)", total_b);
        misses.misses_sort++;
      }
      record_execution(MET_SORT, ev.t_evt_us, t0, t0+dt, total_b,
                       DEADLINE_SORT_US, 0);
    }
  }
}

// ====== SAFETY_TASK (evento Touch D): E-stop ======
static void task_safety(void *arg)
{
  misses.misses_stop = 0;
  misses.total_stop = 0;

  for (;;) {
    int64_t safety_event;
    if (xQueueReceive(qSafetyTimes, &safety_event, portMAX_DELAY) == pdTRUE) {
      misses.total_stop++;
      int64_t t0 = esp_timer_get_time();
      int64_t tempo_touch_stop = t0 - safety_event;

      // Ação crítica: zera "PWM", trava atuadores, sinaliza alarme (~0.9 ms)
      g_belt.set_rpm = 0.f;
      cpu_tight_loop_us(900);

      int64_t dt = esp_timer_get_time() - t0;

      int64_t total_d = tempo_touch_stop + dt;

      ESP_LOGI(TAG, "ESTOP: parada de emergência, exec=%lld microsegundos (us), toque=%lld microssegundos (us), total=%lld microssegundos (us)", dt, tempo_touch_stop, total_d);

      if(total_d > DEADLINE_SAFETY_US){
        ESP_LOGI(TAG, "ESTOP DEADLINE! total=%lld microssegundos (us)", total_d);
        misses.misses_stop++;
      }
      record_execution(MET_SAFE, safety_event, t0, t0+dt, total_d,
                       DEADLINE_SAFETY_US, 0);
    }
  }
}

//callback dos touch
static bool IRAM_ATTR touch_hmi_callback(touch_sensor_handle_t handle, const touch_active_event_data_t *event, void *user_data){
  BaseType_t hp = pdFALSE;

  switch(event->chan_id){
    case 7:{
      sort_evt_t ev7 = {.t_evt_us = esp_timer_get_time()}; //passa o tempo em q foi tocado
      if (xQueueSendFromISR(qSort, &ev7, &hp) != pdTRUE) input_drop(true);
      break;
    }
    case 8: {
      xSemaphoreGiveFromISR(semHMI, &hp);
      break;
    }
    case 9:{
      int64_t safety_event = esp_timer_get_time();
      if (xQueueSendFromISR(qSafetyTimes, &safety_event, &hp) != pdTRUE) input_drop(true);
      break;
    }
    default:{break;}
  }

  return (hp == pdTRUE);
}

static bool IRAM_ATTR touch_inactive_cb(touch_sensor_handle_t h, const touch_inactive_event_data_t *ev, void *ctx)
{
  return false;
}

// ====== app_main ======
void app_main(void)
{
  ESP_ERROR_CHECK(nvs_flash_init());
  wifi_init();

  // Filas da MONITOR_TASK e da tarefa de rede
  qMonitorTx = xQueueCreate(10, sizeof(monitor_msg_t));
  qMonitorCmd = xQueueCreate(10, sizeof(monitor_request_t));

  if (qMonitorTx == NULL || qMonitorCmd == NULL) {
    ESP_LOGE(TAG, "Falha ao criar filas do monitoramento");
    abort();
  }

  // criar os mutex e a fila
  qExecutions = xQueueCreate(128, sizeof(execution_t));
  qSafetyTimes = xQueueCreate(10, sizeof(int64_t));
  qCtrlTimes = xQueueCreate(32, sizeof(int64_t));
  if (!qExecutions || !qSafetyTimes || !qCtrlTimes) abort();
  semHMI   = xSemaphoreCreateBinary();

  qSort = xQueueCreate(10, sizeof(sort_evt_t));
  if (!qSort || !semHMI) abort();

  touch_sensor_handle_t sens_handle = NULL;
  // sample configuration
  touch_sensor_sample_config_t sample_cfg[1] = {
    TOUCH_SENSOR_V1_DEFAULT_SAMPLE_CONFIG(5.0f, TOUCH_VOLT_LIM_L_0V5, TOUCH_VOLT_LIM_H_1V7),
  };
  touch_sensor_config_t touch_cfg = TOUCH_SENSOR_DEFAULT_BASIC_CONFIG(1, sample_cfg);

  //canal do touch HMI/Telemetria
  touch_channel_handle_t ch_b = NULL;
  touch_channel_handle_t ch_c = NULL;
  touch_channel_handle_t ch_d = NULL;

  touch_event_callbacks_t cbs = {
    .on_active = touch_hmi_callback,
    .on_inactive = touch_inactive_cb,
  };
  touch_channel_config_t chan_cfg = {
    .abs_active_thresh = {1000},
    .charge_speed = TOUCH_CHARGE_SPEED_7,
    .init_charge_volt = TOUCH_INIT_CHARGE_VOLT_DEFAULT,
    .group = TOUCH_CHAN_TRIG_GROUP_BOTH
  };
  ESP_ERROR_CHECK(touch_sensor_new_controller(&touch_cfg, &sens_handle));
  // Configura o filtro (obrigatório antes de registrar callbacks)
  touch_sensor_filter_config_t filter_cfg = TOUCH_SENSOR_DEFAULT_FILTER_CONFIG();
  ESP_ERROR_CHECK(touch_sensor_config_filter(sens_handle, &filter_cfg));

  ESP_ERROR_CHECK(touch_sensor_new_channel(sens_handle, 7, &chan_cfg, &ch_b));
  ESP_ERROR_CHECK(touch_sensor_new_channel(sens_handle, 8, &chan_cfg, &ch_c));
  ESP_ERROR_CHECK(touch_sensor_new_channel(sens_handle, 9, &chan_cfg, &ch_d));

  ESP_ERROR_CHECK(touch_sensor_register_callbacks(sens_handle, &cbs, NULL));
  ESP_ERROR_CHECK(touch_sensor_enable(sens_handle));
  ESP_ERROR_CHECK(touch_sensor_start_continuous_scanning(sens_handle));

  // Cria tasks (afinidade no core 0 para reduzir jitter, opcional)
  if (xTaskCreatePinnedToCore(task_safety,   "SAFETY",   STK, NULL, PRIO_ESTOP, &hSAFE, 0) != pdPASS) abort();
  if (xTaskCreatePinnedToCore(task_spd_ctrl, "SPD_CTRL", STK, NULL, PRIO_CTRL,  &hCTRL, 0) != pdPASS) abort();
  hCtrlNotify = hCTRL;
  if (xTaskCreatePinnedToCore(task_enc_sense,"ENC_SENSE",STK, NULL, PRIO_ENC, &hENC, 0) != pdPASS) abort();
  if (xTaskCreatePinnedToCore(task_sort_act, "SORT_ACT", STK, NULL, PRIO_SORT,  &hSORT, 0) != pdPASS) abort();

  //novas tasks:
  // Criar MONITOR_TASK periódica
  BaseType_t ok = xTaskCreate(
    monitor_task,
    "MONITOR_TASK",
    4096,
    NULL,
    PRIO_MON,
    &hMonitor
  );

  if (ok != pdPASS) {
    ESP_LOGE(TAG, "Falha ao criar MONITOR_TASK");
    abort();
  }

  // Criar a tarefa de comunicação escolhida
  #if MONITOR_USE_PROTOCOLO
    ok = xTaskCreate(tcp_task, "NET_TCP", 4096, NULL, 2, &hNet);
  #else
    ok = xTaskCreate(udp_task, "NET_UDP", 4096, NULL, 2, &hNet);
  #endif

  if (ok != pdPASS) {
    ESP_LOGE(TAG, "Falha ao criar tarefa de rede");
    abort();
  }

  time_sntp_init();

  BaseType_t tempo = xTaskCreate(
    time_task,
    "TIME_TASK",
    TIME_STACK,
    NULL,
    TIME_PRIORITY,
    NULL
  );

  if (tempo != pdPASS) {
    ESP_LOGE(TAG_TIME, "Falha ao criar TIME_TASK");
    abort();
  }

}
