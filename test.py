with open('utils.py', 'rb') as f:
    lines = f.readlines()

s = b''
for line in lines:
    s += line  # byte

s = s.decode()  # string
print(type(s))
s = s.encode()  # byte

with open('tmp.py', 'bw') as f:
    f.write(s)