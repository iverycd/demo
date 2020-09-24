import binascii  
a = b'hello world'
b = binascii.b2a_hex(a)
c = binascii.hexlify(a)
d = binascii.a2b_hex('68656c6c6f20776f726c64')
v = memoryview(b'abcefg')
print(a)
print(b)
print(c)
print(d)
print(v[0],v[1],v[2],v[3])
print(bytes(v))
print('hello')
'''

print(b)  
b'68656c6c6f20776f726c64'  
b = binascii.hexlify(a)  
print(b)  
b'68656c6c6f20776f726c64'  
print(binascii.unhexlify(b))  
b'hello sdd'
'''