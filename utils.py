M = 5

def find_offset(initial, final):
    ret = final - initial
    if ret < 0:
        ret = 2 ** M + ret
    return ret