import hashlib


M = 5

PRINT = False

TRACKER_ADDR = 'localhost:40000'


def find_offset(initial, final):
    ret = final - initial
    if ret < 0:
        ret = 2 ** M + ret
    return ret


def get_hash_value(s):
    hash = hashlib.sha1()
    hash.update(str(s).encode())
    # print('hash value of {} is: {}'.format(s, int(hash.hexdigest(), 16) % (2 ** M)))
    return int(hash.hexdigest(), 16) % (2 ** M)


def sha1(s):
    hash = hashlib.sha1()
    hash.update(str(s).encode())
    return hash.hexdigest()


def get_unique_addr_list(n):
    unique = dict()
    ip = 'localhost'
    port = 50000

    while len(unique) != n:
        addr = (ip + ':' + str(port))
        port += 1
        hashed = get_hash_value(addr)
        if hashed not in unique:
            unique[hashed] = addr
    return list(unique.values())