import hashlib


M = 5


def get_hash_value(s):
    hash = hashlib.sha1()
    hash.update(str(s).encode())
    # print('hash value of {} is: {}'.format(s, int(hash.hexdigest(), 16) % (2 ** M)))
    return int(hash.hexdigest(), 16) % (2 ** M)


unique = dict()

ip = 'localhost'
start_port = 50000
for i in range(100):
    addr = (ip + ':' + str(start_port + i))
    hashed = get_hash_value(addr)
    if hashed not in unique:
        unique[hashed] = addr

print(unique)