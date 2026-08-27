import ctypes, os, resource, tempfile, sys
lib = ctypes.CDLL("/usr/lib/libsqlite3.dylib", use_errno=True)
lib.sqlite3_open_v2.argtypes=[ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p), ctypes.c_int, ctypes.c_char_p]
lib.sqlite3_errmsg.restype=ctypes.c_char_p
lib.sqlite3_system_errno.argtypes=[ctypes.c_void_p]; lib.sqlite3_system_errno.restype=ctypes.c_int
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (96, hard))
print("soft now", resource.getrlimit(resource.RLIMIT_NOFILE)[0])
d = tempfile.mkdtemp(prefix="vk68m-exh-")
# open a real WAL store FIRST so the path exists and is a valid db
h0 = ctypes.c_void_p()
p0 = os.path.join(d, "real.sqlite")
print("pre-open rc", lib.sqlite3_open_v2(p0.encode(), ctypes.byref(h0), 0x4|0x2, None))
lib.sqlite3_close_v2(h0)
hogs=[]
try:
    while True:
        hogs.append(os.open("/dev/null", os.O_RDONLY))
except OSError as e:
    print(f"table full after {len(hogs)} hogs; open() errno={e.errno} ({os.strerror(e.errno)})")
# now try sqlite on an EXISTING valid db with the table full
h = ctypes.c_void_p()
rc = lib.sqlite3_open_v2(p0.encode(), ctypes.byref(h), 0x4|0x2, None)
se = lib.sqlite3_system_errno(h) if h else -999
msg = lib.sqlite3_errmsg(h).decode() if h else "?"
print(f"EXHAUSTED existing-db open: rc={rc} system_errno={se} msg={msg!r}")
if h: lib.sqlite3_close_v2(h)
# and a brand new db path with the table full
h2 = ctypes.c_void_p()
p2 = os.path.join(d, "new.sqlite")
rc2 = lib.sqlite3_open_v2(p2.encode(), ctypes.byref(h2), 0x4|0x2, None)
se2 = lib.sqlite3_system_errno(h2) if h2 else -999
msg2 = lib.sqlite3_errmsg(h2).decode() if h2 else "?"
print(f"EXHAUSTED new-db open:      rc={rc2} system_errno={se2} msg={msg2!r}")
