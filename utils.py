import time

'''
4byte DATA_LEN
4byte SEQ_NUM
4byte ACK_NUM
2byte FLAG--SYN/ACK/FIN
2byte CHECK_SUM
nbyte DATA
'''

def cal_checksum(raw_data):
    i = 0
    ret = 0x0
    while i < len(raw_data):
        if 14 == i:
            i += 2
            continue
        ret += int.from_bytes(raw_data[i:i + 2], 'big', signed = False)
        if ret > 0xffff:
            ret -= 0xffff
        i += 2
    return 0xffff - ret

def encode(seq_num = 0, ack_num = 0, flag = 0, data = b''):
    data_len = len(data)
    ret = data_len.to_bytes(4, 'big')
    ret += seq_num.to_bytes(4, 'big')
    ret += ack_num.to_bytes(4, 'big')
    ret += flag.to_bytes(2, 'big')
    ret += (0).to_bytes(2, 'big')
    ret += data
    if len(data) % 2 == 1: # padding
        ret += (0).to_bytes(1, 'big')
    ret = ret[0:14] + cal_checksum(ret).to_bytes(2, 'big') + ret[16:]
    return ret

'''
0 invalid
1 valid
'''

def decode(raw_data):
    assert(len(raw_data) >= 16)
    data_len = int.from_bytes(raw_data[0:4], 'big', signed = False)
    assert(len(raw_data) == data_len + 16 or len(raw_data) == data_len + 17)
    ret = {
        'data': raw_data[16:16 + data_len],
        'flag': int.from_bytes(raw_data[12:14], 'big', signed = False), 
        'seq_num': int.from_bytes(raw_data[4:8], 'big', signed = False),
        'ack_num': int.from_bytes(raw_data[8:12], 'big', signed = False),
        'is_corr': int.from_bytes(raw_data[14:16], 'big', signed = False) != cal_checksum(raw_data)
    }
    return ret    