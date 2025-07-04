from urllib import parse
# from urllib3 import parse

# s = '\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c'
# s = 'Reconstrucci\xc3\xb3n (El Mejor) (Version Acustica)'
s = 'Reconstrucci\\xc3\\xb3n (El Mejor) (Version Acustica)'
print(f"s = {s}")

# s = s.encode('unicode_escape')
# print(f"s = {s}")

# ss = s.decode('utf-8')
# print(f"ss = {ss}")
# ss = ss.replace('\\x', '%')
# print(f"ss = {ss}")

# un = parse.unquote(ss)
# print(f"un = {un}")

#

ss = s.replace('\\x', '%')
print(f"ss = {ss}")
un = parse.unquote(ss)
print(f"un = {un}")

#

# s = s.encode('latin1')
# print(f"s = {s}")

# ss = s.decode('utf-8')
# print(f"ss = {ss}")
