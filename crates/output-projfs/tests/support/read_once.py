import ctypes as c
from ctypes import wintypes as w
import sys
k=c.WinDLL("kernel32",use_last_error=True)
k.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,c.c_void_p,w.DWORD,w.DWORD,w.HANDLE]
k.CreateFileW.restype=w.HANDLE
k.ReadFile.argtypes=[w.HANDLE,c.c_void_p,w.DWORD,c.POINTER(w.DWORD),c.c_void_p]
k.CloseHandle.argtypes=[w.HANDLE]
h=k.CreateFileW(sys.argv[1],0x81,7,None,3,0,None)
if h==c.c_void_p(-1).value:
    print("Old generation open failed:",c.get_last_error())
    sys.exit(0)
try:
    buffer=c.create_string_buffer(4096)
    count=w.DWORD()
    if k.ReadFile(h,buffer,len(buffer),c.byref(count),None):
        data=buffer.raw[:count.value]
        print("Old generation read:",data)
        if len(sys.argv)>2:
            assert data==sys.argv[2].encode(), "old handle returned another generation or sparse bytes"
    else:
        print("Old generation read invalidated:",c.get_last_error())
finally:
    k.CloseHandle(h)
