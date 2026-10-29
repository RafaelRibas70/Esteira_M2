"""Coletor UDP/TCP: SORT, ESTOP, STATUS, PING e QUIT no terminal.
Resultados em resultado.txt junto deste script; cada coleta substitui a anterior.
Use --help para opcoes. Configure o destino TCP nas constantes abaixo.
"""
import argparse
import json
import math
import queue
import socket
import statistics
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path

# Protocolo padrao: 'udp' ou 'tcp'. 
PROTOCOL = 'tcp'

# Destino TCP: preencher com o IP da ESP32, nao o IP do computador.
# --esp-ip e --port no terminal tem prioridade sobre estes valores.
TCP_ESP_IP = '10.245.178.179'
# Porta usada tanto em UDP quanto em TCP; nao e alterada automaticamente.
ESP_PORT = 7000

# Duracao padrao em segundos; 0 coleta ate QUIT ou Ctrl+C.
# --duration no terminal tem prioridade sobre este valor.
COLLECTION_DURATION_SECONDS = 30

TASKS = ['ENC_SENSE', 'SPD_CTRL', 'SORT_ACT', 'SAFETY_TASK', 'MONITOR_TASK', 'TIME_TASK']
PERIODS = {0: 5000, 4: 500000, 5: 1000000}
SOFT = {4, 5}  # Hipotese: monitor e tempo soft; justificar no relatorio.


class TextRows:
    """Secoes identificadas no mesmo TXT; uma linha por registro."""
    def __init__(self, output, section, fields):
        self.output, self.section, self.fields = output, section, fields

    def writerow(self, values):
        fields = []
        for key, value in zip(self.fields, values):
            value = 'sem dados' if value == '' else str(value)
            fields.append(f'{key}={value.replace(chr(13), " ").replace(chr(10), " ")}')
        self.output.write(f'[{self.section}] ' + ' | '.join(fields) + '\n')


def percentile(values, pct):
    # Nearest rank: HWM(100%) equivale ao maximo observado.
    return sorted(values)[max(0, math.ceil(len(values)*pct/100)-1)] if values else ''


def firm_windows(samples, m, k):
    window = deque(maxlen=k)
    previous = None
    windows = violations = 0
    for s in sorted(samples, key=lambda s: s['n']):
        if previous is not None and s['n'] != previous+1:
            window.clear()  # Nao inferir cumprimento nas lacunas da coleta.
        previous = s['n']
        window.append(not s['miss'])
        if len(window) == k:
            windows += 1
            violations += sum(window) < m
    return windows, violations


class Collector:
    def __init__(self, directory, protocol, m=9, k=10):
        self.directory, self.protocol = directory, protocol
        self.m, self.k = m, k
        self.samples = defaultdict(dict)
        self.stats, self.health, self.pending = {}, {}, {}
        self.cpu, self.rtts = [], []
        self.invalid = 0
        self.received = 0
        self.closed = False
        self.path = directory / 'resultado.txt'
        # Line buffering: os registros ficam visiveis enquanto a coleta acontece.
        self.output = self.path.open('w', encoding='utf-8', buffering=1)
        self.output.write('RESULTADOS DA COLETA - ESTEIRA INDUSTRIAL\n'
                          f'Inicio: {datetime.now().isoformat(timespec="seconds")}\n'
                          f'Protocolo: {protocol} | (m,k): ({m},{k})\n'
                          'Timestamps monotonicos em us; horarios civis em ms Unix.\n'
                          'As secoes RESUMO e GERAL sao gravadas ao encerrar com QUIT/Ctrl+C ou pelo tempo definido.\n\n')
        self.events = self.writer('EXECUCAO', ['protocolo', 'pc_rx_ms', 'seq', 'task', 'n',
            'timestamp_evento_us', 'timestamp_task_start_us', 'timestamp_task_end_us',
            'tempo_deadline_us', 'resposta_evento_us', 'latencia_us', 'miss', 'bytes'])
        self.messages = self.writer('MENSAGEM', ['protocolo', 'pc_rx_ms', 'bytes',
            'esp_tx_ms', 'sntp_sync', 'diferenca_relogios_ms', 'json'])
        self.communication = self.writer('COMUNICACAO', ['protocolo', 'ping',
            'pc_tx_ms', 'pc_rx_ms', 'rtt_ms', 'status'])

    def writer(self, name, fields):
        return TextRows(self.output, name, fields)

    def diagnostic(self, message):
        self.output.write(f'[DIAGNOSTICO] {message}\n')
        print(message)

    def receive(self, data, rx_ms, rx_ns):
        self.received += 1
        try:
            msg = json.loads(data.decode('utf-8'))
            if not isinstance(msg, dict):
                raise ValueError('JSON deve ser objeto')
            tx = msg.get('tx_ms', '')
            delta = rx_ms-tx if msg.get('sync') and isinstance(tx, (int, float)) else ''
            self.messages.writerow([self.protocol, rx_ms, len(data), tx,
                msg.get('sync', ''), delta, json.dumps(msg, ensure_ascii=False)])
            if msg.get('type') == 'exec':
                task = int(msg['task'])
                if task not in range(len(TASKS)):
                    raise ValueError('task desconhecida')
                s = {key: int(msg[key]) for key in ('n', 'e', 's', 'f', 'r', 'miss')}
                if not 0 <= s['e'] <= s['s'] <= s['f']:
                    raise ValueError('timestamps invalidos')
                self.samples[task][s['n']] = s
                self.events.writerow([self.protocol, rx_ms, msg['seq'], TASKS[task], s['n'],
                    s['e'], s['s'], s['f'], s['r'], s['f']-s['e'], s['s']-s['e'], s['miss'], len(data)])
            elif msg.get('type') == 'stats':
                task = int(msg['task'])
                if msg['n'] >= self.stats.get(task, {}).get('n', -1):
                    self.stats[task] = msg
            elif msg.get('type') == 'health':
                self.health = msg
            elif msg.get('type') == 'cpu':
                self.cpu.append(float(msg['busy_pct']))
            elif msg.get('type') == 'PONG':
                key = msg.get('echo', '')
                pending = self.pending.pop(key, None)
                if pending:
                    sent_ns, sent_ms = pending
                    rtt = (rx_ns-sent_ns)/1_000_000
                    self.rtts.append(rtt)
                    self.communication.writerow([self.protocol, key, sent_ms, rx_ms, rtt, 'ok'])
                    print(f'{key}: RTT {rtt:.3f} ms')
        except (ValueError, KeyError, TypeError, UnicodeError) as exc:
            self.invalid += 1
            self.diagnostic(f'Registro invalido: {exc}; pc_rx_ms={rx_ms}; bytes={len(data)}; bruto={data!r}')

    def expire_pings(self, now_ns, timeout=5):
        for key, (sent_ns, sent_ms) in list(self.pending.items()):
            if now_ns-sent_ns >= timeout*1_000_000_000:
                self.communication.writerow([self.protocol, key, sent_ms, '', '', 'timeout'])
                del self.pending[key]

    def finish(self):
        if self.closed:
            return
        self.expire_pings(time.monotonic_ns(), timeout=0)
        self.output.write('\nRESUMO FINAL POR TAREFA\n')
        summary_fields = ['protocolo', 'task', 'execucoes_coletadas',
            'misses_coletados', 'miss_pct', 'wcrt_evento_us', 'hwm99_us', 'max_tempo_deadline_us',
            'latencia_max_us', 'jitter_max_us', 'm', 'k', 'janelas_completas', 'violacoes_mk',
            'lacunas_internas', 'total_esp_ultimo_resumo', 'misses_esp_ultimo_resumo', 'wcrt_esp_ultimo_resumo_us']
        for task, name in enumerate(TASKS):
            samples = sorted(self.samples[task].values(), key=lambda s: s['n'])
            response = [s['f']-s['e'] for s in samples]
            jitter = [abs(b['s']-a['s']-PERIODS[task]) for a, b in zip(samples, samples[1:])
                      if task in PERIODS and b['n'] == a['n']+1]
            windows, violations = firm_windows(samples, self.m, self.k) if task in SOFT else ('', '')
            gaps = sum(max(0, b['n']-a['n']-1) for a, b in zip(samples, samples[1:]))
            misses = sum(s['miss'] for s in samples)
            device = self.stats.get(task, {})
            values = [self.protocol, name, len(samples), misses,
                misses*100/len(samples) if samples else '', max(response, default=''), percentile(response, 99),
                max((s['r'] for s in samples), default=''), max((s['s']-s['e'] for s in samples), default=''),
                max(jitter, default=''), self.m if task in SOFT else '', self.k if task in SOFT else '',
                windows, violations, gaps, device.get('n', ''), device.get('misses', ''), device.get('wcrt', '')]
            # Omitir somente metricas que nao se aplicam; ausencia de amostras
            # em uma metrica aplicavel continua indicada como 'sem dados'.
            excluded = set()
            if task not in PERIODS:
                excluded.add('jitter_max_us')
            if task not in SOFT:
                excluded.update(('m', 'k', 'janelas_completas', 'violacoes_mk'))
            applicable = [(field, value) for field, value in zip(summary_fields, values)
                          if field not in excluded]
            self.writer('RESUMO', [field for field, _ in applicable]).writerow(
                [value for _, value in applicable])
        self.output.write('\nRESULTADOS GERAIS DA COLETA\n')
        w = self.writer('GERAL', ['protocolo', 'cpu_media_pct', 'cpu_max_pct',
            'rtt_medio_ms', 'rtt_max_ms', 'rtt_p99_ms', 'registros_invalidos', 'health_final', 'mensagens_recebidas'])
        w.writerow([self.protocol, statistics.mean(self.cpu) if self.cpu else '',
            max(self.cpu, default=''), statistics.mean(self.rtts) if self.rtts else '',
            max(self.rtts, default=''), percentile(self.rtts, 99), self.invalid, json.dumps(self.health), self.received])
        if not self.received:
            self.diagnostic('Nenhuma mensagem recebida: nao ha medicoes para calcular. '
                            'Confira protocolo, porta, PC_IP do firmware, WiFi e firewall do PC.')
        elif not any(self.samples.values()):
            self.diagnostic('Mensagens recebidas, mas nenhum registro type=exec. '
                            'Confira se o firmware gravado envia os registros de execucao.')
        self.output.write(f'Fim: {datetime.now().isoformat(timespec="seconds")}\n')
        self.output.close()
        self.closed = True


def console_reader(commands):
    while True:
        try:
            commands.put(input().strip().upper())
        except (EOFError, OSError):
            return


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--protocol', choices=('udp', 'tcp'), default=PROTOCOL,
                   help='Protocolo; padrao definido em PROTOCOL no codigo')
    p.add_argument('--esp-ip', help='IP da ESP32; TCP usa TCP_ESP_IP quando omitido')
    p.add_argument('--bind', default='0.0.0.0')
    p.add_argument('--port', type=int, help='Porta informada explicitamente; padrao: ESP_PORT no codigo')
    p.add_argument('--duration', type=float, default=COLLECTION_DURATION_SECONDS,
                   help='Segundos; padrao: COLLECTION_DURATION_SECONDS no codigo; 0 ate QUIT/Ctrl+C')
    p.add_argument('--ping-interval', type=float, default=1, help='Segundos; 0 desativa pings automaticos')
    p.add_argument('--m', type=int, default=9)
    p.add_argument('--k', type=int, default=10)
    args = p.parse_args()
    if args.protocol not in ('udp', 'tcp'):
        p.error("PROTOCOL deve ser 'udp' ou 'tcp'")
    if args.protocol == 'tcp':
        args.esp_ip = args.esp_ip or TCP_ESP_IP
    if not 1 <= args.m <= args.k or args.duration < 0 or args.ping_interval < 0:
        p.error('Exige 1 <= m <= k, duration >= 0 e ping-interval >= 0')
    if args.protocol == 'tcp' and not args.esp_ip:
        p.error('Preencha TCP_ESP_IP no inicio do arquivo ou informe --esp-ip')
    port = args.port if args.port is not None else ESP_PORT
    directory = Path(__file__).resolve().parent
    c = Collector(directory, args.protocol.upper(), args.m, args.k)
    commands = queue.Queue()
    threading.Thread(target=console_reader, args=(commands,), daemon=True).start()
    peer = (args.esp_ip, port) if args.esp_ip else None
    start = next_ping = last_receive = last_status = time.monotonic()
    ping_id = 0
    buffer = b''
    try:
        if args.esp_ip:
            peer = (socket.gethostbyname(args.esp_ip), port)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM if args.protocol == 'udp' else socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            if args.protocol == 'udp':
                sock.bind((args.bind, port))
            else:
                sock.connect(peer)
            sock.settimeout(0.05)
            c.diagnostic(f'Coletando {args.protocol.upper()} na porta {port}. TXT: {c.path}')
            print('Comandos: SORT, ESTOP, STATUS, PING, QUIT. Reinicie a ESP32 a cada coleta.')
            while not args.duration or time.monotonic()-start < args.duration:
                now = time.monotonic()
                if args.ping_interval and now >= next_ping and peer:
                    commands.put('PING')
                    next_ping = now+args.ping_interval
                try:
                    command = commands.get_nowait()
                except queue.Empty:
                    command = None
                if command == 'QUIT':
                    break
                if command:
                    if command not in ('SORT', 'ESTOP', 'STATUS', 'PING'):
                        print('Comando desconhecido')
                    elif peer is None:
                        print('Aguardando IP da ESP32; use --esp-ip para informar manualmente')
                    else:
                        if command == 'PING':
                            command = f'PING {ping_id}'
                            ping_id += 1
                            c.pending[command] = (time.monotonic_ns(), time.time_ns()//1_000_000)
                        payload = command.encode('ascii')
                        if args.protocol == 'udp':
                            sock.sendto(payload, peer)
                        else:
                            sock.sendall(payload+b'\n')
                        c.output.write(f'[ENVIO] pc_tx_ms={time.time_ns()//1_000_000} | destino={peer} | comando={command}\n')
                try:
                    if args.protocol == 'udp':
                        data, source = sock.recvfrom(2048)
                        # Aprende apenas o IP da placa; preserva a porta configurada.
                        if peer is not None and source[0] != peer[0]:
                            c.diagnostic(f'Pacote ignorado de {source}: IP esperado {peer[0]}')
                            continue
                        peer = (source[0], port)
                        packets = [data]
                    else:
                        data = sock.recv(4096)
                        if not data:
                            c.diagnostic('ESP32 encerrou a conexao')
                            if buffer:
                                c.diagnostic(f'Linha TCP incompleta ao desconectar: {buffer!r}')
                            break
                        buffer += data
                        *packets, buffer = buffer.split(b'\n')
                        if len(buffer) > 8192:
                            raise ValueError('Linha TCP excedeu limite de recepcao')
                    rx_ms, rx_ns = time.time_ns()//1_000_000, time.monotonic_ns()
                    for packet in packets:
                        if packet:
                            c.receive(packet, rx_ms, rx_ns)
                            if c.received == 1:
                                c.diagnostic(f'Primeira mensagem recebida de {peer}; gravacao em andamento.')
                            last_receive = time.monotonic()
                except socket.timeout:
                    pass
                c.expire_pings(time.monotonic_ns())
                if time.monotonic()-last_status >= 5:
                    idle = time.monotonic()-last_receive
                    c.diagnostic(f'Mensagens recebidas: {c.received}; registros invalidos: {c.invalid}.')
                    if idle >= 5:
                        c.diagnostic(f'Sem mensagens ha {idle:.0f}s na porta {port}. '
                                     'Confira PC_IP/PC_PORT no firmware gravado, protocolo, WiFi e firewall.')
                    last_status = time.monotonic()
    except KeyboardInterrupt:
        pass
    except (OSError, ValueError) as exc:
        c.diagnostic(f'Coleta interrompida: {exc}')
    finally:
        c.finish()
        print(f'Resultados salvos em {c.path}')


if __name__ == '__main__':
    main()
