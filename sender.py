import queue
import random
import signal
import socket
import sys
import threading
import time
import utils

config = {}
is_finished = False
mutex = threading.Lock()

def log(event, type_of_packet, seq_num, size_of_data, ack_num):
    t = ''
    if type_of_packet == 0:
        t = 'D'
    elif type_of_packet == 1:
        t = 'F'
    else:
        if type_of_packet & 4:
            t += 'S'
        if type_of_packet & 2:
            t += 'A'
    type_of_packet = t

    if 'drop' in event:
        config['nb_seg_sent'] += 1
        config['nb_seg_drop'] += 1
    if 'snd' in event:
        config['nb_seg_sent'] += 1
        if 'corr' in event:
            config['nb_seg_corr'] += 1
        if 'rord' in event:
            config['nb_seg_reorder'] += 1
        if 'dup' in event:
            config['nb_seg_dup'] += 1
        if 'dely' in event:
            config['nb_seg_dely'] += 1
    if 'rcv' in event:
        if 'DA' in event:
            config['nb_dup_ack'] += 1
    with mutex:
        config['log_writer'].write('%-28s' % event)
        config['log_writer'].write('%7.2f' % (time.time() - config['time']))
        config['log_writer'].write('       ')
        config['log_writer'].write('%-22s' % type_of_packet)
        config['log_writer'].write('%5d' % seq_num)
        config['log_writer'].write('%17d' % size_of_data)
        config['log_writer'].write('%17d' % ack_num)
        config['log_writer'].write('\n')

def quit(signum, frame):
    print('Quit.')
    exit(0)

def sender_recv(sender_socket):
    while True:
        try:
            raw_data, addr = sender_socket.recvfrom(65536)
        except BlockingIOError:
            continue
        return utils.decode(raw_data)

def handshake(sender_socket):
    seq_num = 0
    ack_num = 0
    config['time'] = time.time()
    sender_socket.sendto(utils.encode(seq_num, ack_num, 4), (config['receiver_host_ip'], config['receiver_port']))
    log('snd', 4, seq_num, 0, ack_num)
    seq_num += 1

    ret = sender_recv(sender_socket)
    assert(ret is not None)
    assert(ret['ack_num'] == seq_num)
    assert(ret['flag'] == 6)
    log('rcv', ret['flag'], ret['seq_num'], len(ret['data']), ret['ack_num'])
    ack_num = ret['seq_num'] + 1

    sender_socket.sendto(utils.encode(seq_num, ack_num, 2), (config['receiver_host_ip'], config['receiver_port']))
    log('snd', 2, seq_num, 0, ack_num)

    return seq_num, ack_num

def handwave(sender_socket, seq_num, ack_num):
    sender_socket.sendto(utils.encode(seq_num, ack_num, 1), (config['receiver_host_ip'], config['receiver_port']))
    log('snd', 1, seq_num, 0, ack_num)
    seq_num += 1

    ret = sender_recv(sender_socket)
    assert(ret is not None)
    assert(ret['ack_num'] == seq_num) #
    assert(ret['flag'] == 2)
    log('rcv', ret['flag'], ret['seq_num'], len(ret['data']), ret['ack_num'])

    ret = sender_recv(sender_socket)
    assert(ret is not None)
    assert(ret['seq_num'] == ack_num)
    assert(ret['flag'] == 1)
    log('rcv', ret['flag'], ret['seq_num'], len(ret['data']), ret['ack_num'])
    ack_num += 1

    sender_socket.sendto(utils.encode(seq_num, ack_num, 2), (config['receiver_host_ip'], config['receiver_port']))
    log('snd', 2, seq_num, 0, ack_num)
    sender_socket.close()

def load_file():
    data = b''
    with open(config['filename'], 'rb+') as f:
        t = f.read(65536)
        while len(t) != 0:
            data += t
            t = f.read(65536)
    return data

def PLD_send(sender_socket, q):
    delay_queue = []
    reordered_packet = None
    counter = -1 # avoid maxOrder == 0
    while not is_finished:
        for obj in delay_queue:
            if is_finished:
                break
            if obj[3] >= 1000 * time.time():
                log('snd/dely', 0, obj[0], len(obj[2]), obj[1])
                sender_socket.sendto(utils.encode(obj[0], obj[1], 0, obj[2]), (config['receiver_host_ip'], config['receiver_port']))
                counter += (0 if reordered_packet is None else 1)
        if counter >= config['maxOrder']:
            log('snd/rord', 0, reordered_packet[0], len(reordered_packet[2]), reordered_packet[1])
            sender_socket.sendto(utils.encode(reordered_packet[0], reordered_packet[1], 0, reordered_packet[2]), (config['receiver_host_ip'], config['receiver_port']))
            counter = -1
            reordered_packet = None
        if not q.empty():
            config['nb_seg_pld'] += 1
            seq_num, ack_num, data, is_rxt = q.get()
            if random.random() <= config['pDrop']:
                log('drop', 0, seq_num, len(data), ack_num)
                continue
            elif random.random() <= config['pDuplicate']:
                log('snd' + ('/RXT' if is_rxt else ''), 0, seq_num, len(data), ack_num)
                sender_socket.sendto(utils.encode(seq_num, ack_num, 0, data), (config['receiver_host_ip'], config['receiver_port']))
                log('snd/dup', 0, seq_num, len(data), ack_num)
                sender_socket.sendto(utils.encode(seq_num, ack_num, 0, data), (config['receiver_host_ip'], config['receiver_port']))
                counter += (0 if reordered_packet is None else 2)
                continue
            elif random.random() <= config['pCorrupt']:
                log('snd/corr', 0, seq_num, len(data), ack_num)
                data = utils.encode(seq_num, ack_num, 0, data)
                if data[16] & 1:
                    data = data[:16] + (data[16] & 0xfe).to_bytes(1, 'big') + data[17:]
                else:
                    data = data[:16] + (data[16] | 1).to_bytes(1, 'big') + data[17:]
                sender_socket.sendto(data, (config['receiver_host_ip'], config['receiver_port']))
                counter += (0 if reordered_packet is None else 1)
            elif random.random() <= config['pOrder']:
                if reordered_packet is not None:
                    log('snd' + ('/RXT' if is_rxt else ''), 0, seq_num, len(data), ack_num)
                    sender_socket.sendto(utils.encode(seq_num, ack_num, 0, data), (config['receiver_host_ip'], config['receiver_port']))
                    counter += (0 if reordered_packet is None else 1)
                else:
                    counter = 0
                    reordered_packet = (seq_num, ack_num, data)
            elif random.random() <= config['pDelay']:
                delay_queue.append((seq_num, ack_num, data, time.time() * 1000 + random.randint(0, config['maxDelay'])))
            else:
                log('snd' + ('/RXT' if is_rxt else ''), 0, seq_num, len(data), ack_num)
                sender_socket.sendto(utils.encode(seq_num, ack_num, 0, data), (config['receiver_host_ip'], config['receiver_port']))
                counter += (0 if reordered_packet is None else 1)

def transfer(sender_socket):
    window_size = 0
    data = load_file()
    config['size_of_file'] = len(data)
    left = 0
    right = 0
    counter = 0
    packets = {}
    q = queue.Queue()
    t = threading.Thread(target = PLD_send, args = (sender_socket, q))
    t.setDaemon(False)
    t.start()
    SampleRTT = 0
    EstimatedRTT = 500
    DevRTT = 250
    RTO = EstimatedRTT + config['gamma'] * DevRTT

    init_seq_num, init_ack_num = handshake(sender_socket)

    timer = time.time()
    while left < len(data):
        if 1000 * (time.time() - timer) >= RTO: #retransmission due to timeout
            counter = 0
            config['nb_seg_timeout'] += 1
            q.put((init_seq_num + left, init_ack_num, data[left: left + min(config['MSS'], right - left)], True))
            timer = time.time()
            if left in packets:
                packets.pop(left)
        try:
            raw_data, addr = sender_socket.recvfrom(65536)
            ret = utils.decode(raw_data)
            assert(ret is not None)
            assert(ret['seq_num'] == init_ack_num)
            assert(ret['flag'] == 2)
            if ret['ack_num'] - init_seq_num == left:
                log('rcv/DA', 2, ret['seq_num'], len(ret['data']), ret['ack_num'])
                counter += 1
                if counter == 3: # fast retransmission
                    config['nb_seg_fast_rxt'] += 1
                    q.put((init_seq_num + left, init_ack_num, data[left: left + min(config['MSS'], right - left)], True))
                    timer = time.time()
                    if left in packets:
                        packets.pop(left)
            elif ret['ack_num'] - init_seq_num > left:
                log('rcv', 2, ret['seq_num'], len(ret['data']), ret['ack_num'])
                counter = 0
                if left in packets:
                    SampleRTT = 1000 * (time.time() - packets[left])
                    EstimatedRTT = 0.875 * EstimatedRTT + 0.125 * SampleRTT
                    DevRTT = 0.75 * DevRTT + 0.25 * abs(SampleRTT - EstimatedRTT)
                    RTO = EstimatedRTT + config['gamma'] * DevRTT
                    packets.pop(left)
                left = ret['ack_num'] - init_seq_num
                timer = time.time()
            continue
        except BlockingIOError:
            pass
        len_of_data_to_send = min(config['MSS'], config['MWS'] - right + left, len(data) - right)
        if len_of_data_to_send != 0:
            data_to_send = data[right: right + len_of_data_to_send]
            q.put((init_seq_num + right, init_ack_num, data_to_send, False))
            packets[right] = time.time()
            right += len_of_data_to_send

    assert(left == len(data))
    assert(right == len(data))

    global is_finished
    is_finished = True
    t.join()
    while True:
        try:
            raw_data, addr = sender_socket.recvfrom(65536)
        except BlockingIOError:
            break
    handwave(sender_socket, init_seq_num + left, init_ack_num)

    sender_socket.close()
    t.join()

def write_stat():
    config['log_writer'].write('=============================================================\n')
    config['log_writer'].write('Size of the file (in Bytes)%34d\n' % config['size_of_file'])
    config['log_writer'].write('Segments transmitted (including drop & RXT)%18d\n' % config['nb_seg_sent'])
    config['log_writer'].write('Number of Segments handled by PLD%28d\n' % config['nb_seg_pld'])
    config['log_writer'].write('Number of Segments dropped%35d\n' % config['nb_seg_drop'])
    config['log_writer'].write('Number of Segments Corrupted%33d\n' % config['nb_seg_corr'])
    config['log_writer'].write('Number of Segments Re-ordered%32d\n' % config['nb_seg_reorder'])
    config['log_writer'].write('Number of Segments Duplicated%32d\n' % config['nb_seg_dup'])
    config['log_writer'].write('Number of Segments Delayed%35d\n' % config['nb_seg_dely'])
    config['log_writer'].write('Number of Retransmissions due to TIMEOUT%21d\n' % config['nb_seg_timeout'])
    config['log_writer'].write('Number of FAST RETRANSMISSION%32d\n' % config['nb_seg_fast_rxt'])
    config['log_writer'].write('Number of DUP ACKS received%34d\n' % config['nb_dup_ack'])
    config['log_writer'].write('=============================================================\n')

    config['log_writer'].close()

if __name__ == '__main__':
    assert(len(sys.argv) == 15)
    config['receiver_host_ip'] = sys.argv[1]
    config['receiver_port'   ] = int(sys.argv[2])
    config['filename'        ] = sys.argv[3]
    config['MWS'             ] = int(sys.argv[4])
    config['MSS'             ] = int(sys.argv[5])
    config['gamma'           ] = int(sys.argv[6])
    config['pDrop'           ] = float(sys.argv[7])
    config['pDuplicate'      ] = float(sys.argv[8])
    config['pCorrupt'        ] = float(sys.argv[9])
    config['pOrder'          ] = float(sys.argv[10])
    config['maxOrder'        ] = int(sys.argv[11])
    config['pDelay'          ] = float(sys.argv[12])
    config['maxDelay'        ] = int(sys.argv[13])
    config['seed'            ] = int(sys.argv[14])
    config['log_writer'      ] = open('Sender_log.txt', 'w+')
    config['size_of_file'    ] = 0
    config['nb_seg_sent'     ] = 0
    config['nb_seg_pld'      ] = 0
    config['nb_seg_drop'     ] = 0
    config['nb_seg_corr'     ] = 0
    config['nb_seg_reorder'  ] = 0
    config['nb_seg_dup'      ] = 0
    config['nb_seg_dely'     ] = 0
    config['nb_seg_timeout'  ] = 0 # TODO
    config['nb_seg_fast_rxt' ] = 0 # TODO
    config['nb_dup_ack'      ] = 0
    random.seed(config['seed'])
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender_socket.settimeout(0.0)
    print('Sending...')
    t = threading.Thread(target = transfer, args = (sender_socket,))
    t.setDaemon(False)
    t.start()
    while(t.isAlive()):
        time.sleep(1)
    write_stat()