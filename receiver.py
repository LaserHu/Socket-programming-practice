import random
import signal
import socket
import sys
import threading
import time
import utils

config = {}
is_finished = False

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

    if 'rcv' in event:
        config['segment'] += 1
        if 'D' in type_of_packet:
            config['data_recv'] += size_of_data
            config['data_seg'] += 1
        if 'corr' in event:
            config['corr_seg'] += 1
    if 'DA' in event:
        config['dup_ack_sent'] += 1
    config['log_writer'].write('%-28s' % event)
    config['log_writer'].write('%7.2f' % (time.time() - config['time']))
    config['log_writer'].write('       ')
    config['log_writer'].write('%-22s' % type_of_packet)
    config['log_writer'].write('%5d' % seq_num)
    config['log_writer'].write('%17d' % size_of_data)
    config['log_writer'].write('%17d' % ack_num)
    config['log_writer'].write('\n')

def quit(signum, frame):
    global is_finished
    is_finished = True
    print('Quit.')
    exit(0)

def handshake(receiver_socket):
    raw_data, addr = receiver_socket.recvfrom(65536)
    config['time'] = time.time()
    config['sender_addr'] = addr
    ret = utils.decode(raw_data)
    assert(ret is not None)
    assert(ret['flag'] == 4)
    log('rcv', ret['flag'], ret['seq_num'], len(ret['data']), ret['ack_num'])
    seq_num = 0
    ack_num = ret['seq_num'] + 1

    receiver_socket.sendto(utils.encode(seq_num, ack_num, 6), config['sender_addr'])
    log('snd', 6, seq_num, 0, ack_num)
    seq_num += 1

    raw_data, addr = receiver_socket.recvfrom(65536)
    ret = utils.decode(raw_data)
    assert(ret is not None)
    assert(ret['ack_num'] == seq_num)
    assert(ret['flag'] == 2)
    log('rcv', ret['flag'], ret['seq_num'], len(ret['data']), ret['ack_num'])

    return seq_num, ack_num

def handwave(receiver_socket, seq_num, ack_num):
    receiver_socket.sendto(utils.encode(seq_num, ack_num, 2), config['sender_addr'])
    log('snd', 2, seq_num, 0, ack_num)
    
    receiver_socket.sendto(utils.encode(seq_num, ack_num, 1), config['sender_addr'])
    log('snd', 1, seq_num, 0, ack_num)
    seq_num += 1

    raw_data, addr = receiver_socket.recvfrom(65536)
    ret = utils.decode(raw_data)
    assert(ret is not None)
    assert(ret['ack_num'] == seq_num)
    assert(ret['flag'] == 2)
    log('rcv', ret['flag'], ret['seq_num'], len(ret['data']), ret['ack_num'])

    receiver_socket.close()

def buffer_packet(buffer, packet):
    if len(buffer) == 0 or buffer[len(buffer) - 1]['seq_num'] < packet['seq_num']:
        buffer.append(packet)
    elif packet['seq_num'] < buffer[0]['seq_num']:
        buffer.insert(0, packet)
    else:
        for i in range(len(buffer)):
            if buffer[i]['seq_num'] == packet['seq_num']:
                config['dup_data_seg' ] += 1 #special
                break
            elif buffer[i]['seq_num'] < packet['seq_num'] and packet['seq_num'] < buffer[i + 1]['seq_num']:
                buffer.insert(i, packet)
                break
    while len(buffer) > 1:
        if buffer[0]['seq_num'] + len(buffer[0]['data']) > buffer[1]['seq_num']:
            buffer.pop(1)
        elif buffer[0]['seq_num'] + len(buffer[0]['data']) == buffer[1]['seq_num']:
            buffer[0]['data'] += buffer[1]['data']
            buffer.pop(1)
        else:
            break

def listening(receiver_socket):
    seq_num, ack_num = handshake(receiver_socket)
    buffer = []
    f = open(config['filename'], 'wb+')

    while not is_finished:
        raw_data, addr = receiver_socket.recvfrom(65536)
        ret = utils.decode(raw_data)
        if ret['is_corr']:
            log('rcv/corr', ret['flag'], ret['seq_num'], len(ret['data']), ret['ack_num'])
            continue
        assert(ret['ack_num'] == seq_num)
        if ret['flag'] == 1:
            assert(ret['seq_num'] == ack_num)
            log('rcv', ret['flag'], ret['seq_num'], len(ret['data']), ret['ack_num'])
            ack_num += 1
            handwave(receiver_socket, seq_num, ack_num)
            break
        assert(ret['flag'] == 0)
        log('rcv', ret['flag'], ret['seq_num'], len(ret['data']), ret['ack_num'])
        buffer_packet(buffer, ret)
        if buffer[0]['seq_num'] <= ack_num:
            f.write(buffer[0]['data'][ack_num - buffer[0]['seq_num']:])
            ack_num += len(buffer[0]['data'][ack_num - buffer[0]['seq_num']:])
            buffer.pop(0)
            log('snd', 2, seq_num, 0, ack_num)
        else:
            log('snd/DA', 2, seq_num, 0, ack_num)
        receiver_socket.sendto(utils.encode(seq_num, ack_num, 2), config['sender_addr'])

    f.close()
    receiver_socket.close()

def write_stat():
    config['log_writer'].write('==============================================\n')
    config['log_writer'].write('Amount of data received (bytes)%15d\n' % config['data_recv'])
    config['log_writer'].write('Total Segments Received%23d\n' % config['segment'])
    config['log_writer'].write('Data segments received%24d\n' % config['data_seg'])
    config['log_writer'].write('Data segments with Bit Errors%17d\n' % config['corr_seg'])
    config['log_writer'].write('Duplicate data segments received%14d\n' % config['dup_data_seg'])
    config['log_writer'].write('Duplicate ACKs sent%27d\n' % config['dup_ack_sent'])
    config['log_writer'].write('==============================================\n')

    config['log_writer'].close()

if __name__ == '__main__':
    assert(len(sys.argv) == 3)
    config['receiver_port'] = int(sys.argv[1])
    config['filename'     ] = sys.argv[2]
    config['log_writer'   ] = open('Receiver_log.txt', 'w+')
    config['data_recv'    ] = 0
    config['segment'      ] = 0
    config['data_seg'     ] = 0
    config['corr_seg'     ] = 0
    config['dup_data_seg' ] = 0
    config['dup_ack_sent' ] = 0
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver_socket.bind(('127.0.0.1', config['receiver_port']))
    print('Listening...')
    t = threading.Thread(target = listening, args = (receiver_socket,))
    t.setDaemon(False)
    t.start()
    while(t.isAlive()):
        time.sleep(1)
    write_stat()