"""External native client: cancel concurrent opens/reads through actual ProjFS."""
import ctypes as c
from ctypes import wintypes as w
import concurrent.futures
import queue
import pathlib
import sys
import threading
import time

k = c.WinDLL("kernel32", use_last_error=True)
class Overlapped(c.Structure):
    _fields_ = [("Internal", c.c_size_t), ("InternalHigh", c.c_size_t),
                ("Offset", w.DWORD), ("OffsetHigh", w.DWORD), ("hEvent", w.HANDLE)]
k.CreateFileW.argtypes = [w.LPCWSTR,w.DWORD,w.DWORD,c.c_void_p,w.DWORD,w.DWORD,w.HANDLE]
k.CreateFileW.restype = w.HANDLE
k.ReadFile.argtypes = [w.HANDLE,c.c_void_p,w.DWORD,c.POINTER(w.DWORD),c.POINTER(Overlapped)]
k.CancelIoEx.argtypes = [w.HANDLE,c.c_void_p]
k.CancelSynchronousIo.argtypes = [w.HANDLE]
k.OpenThread.argtypes = [w.DWORD,w.BOOL,w.DWORD]
k.OpenThread.restype = w.HANDLE
k.GetOverlappedResult.argtypes = [w.HANDLE,c.POINTER(Overlapped),c.POINTER(w.DWORD),w.BOOL]
k.CloseHandle.argtypes = [w.HANDLE]
shutdown = sys.argv[1:2] == ["--shutdown"]
paths = sys.argv[2:] if shutdown else sys.argv[1:]
threads = queue.Queue()
files = []

def read(path):
    threads.put(threading.get_native_id())
    # ProjFS/other filters may block CreateFile itself even for an overlapped
    # handle. Run opens concurrently and cancel that synchronous part too.
    h = k.CreateFileW(path,0x81,7,None,3,0x40000000,None)
    if h == c.c_void_p(-1).value:
        return c.get_last_error()
    files.append(h)
    overlapped = Overlapped()
    buffer = c.create_string_buffer(4096)
    count = w.DWORD()
    ok = k.ReadFile(h,buffer,len(buffer),c.byref(count),c.byref(overlapped))
    error = c.get_last_error()
    if ok:
        expected=int(pathlib.Path(path).name.split("-")[-1])+(32 if pathlib.Path(path).name.startswith("stop-") else 1)
        assert count.value==4096 and buffer.raw[:count.value]==bytes([expected])*count.value, (path,count.value,buffer.raw[:16].hex())
        return 0
    if error != 997:
        return error
    ok = k.GetOverlappedResult(h,c.byref(overlapped),c.byref(count),True)
    if ok:
        expected=int(pathlib.Path(path).name.split("-")[-1])+(32 if pathlib.Path(path).name.startswith("stop-") else 1)
        assert count.value==4096 and buffer.raw[:count.value]==bytes([expected])*count.value, (path,count.value,buffer.raw[:16].hex())
        return 0
    return c.get_last_error()

thread_handles=[]
try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(paths)) as pool:
        futures=[pool.submit(read,path) for path in paths]
        for _ in futures:
            thread=k.OpenThread(1,False,threads.get(timeout=5))
            assert thread, c.get_last_error()
            thread_handles.append(thread)
        time.sleep(.1)
        deadline=time.monotonic()+5
        while not shutdown and not all(f.done() for f in futures) and time.monotonic()<deadline:
            for thread in thread_handles:
                k.CancelSynchronousIo(thread)
            for h in files:
                k.CancelIoEx(h,None)
            time.sleep(.01)
        results=[f.result(timeout=10) for f in futures]
    if shutdown:
        assert all(error in (0,369,372,995,8) for error in results), results
        print(f"Shutdown drained {len(results)} native opens/reads: {results}")
    else:
        # Completion may win cancellation in the filesystem/filter stack. Native
        # callers must receive either correct bytes or ERROR_OPERATION_ABORTED.
        assert all(error in (0,995) for error in results), results
        print(f"Cancellation attempted for {len(results)} opens/reads: {results}")
finally:
    for h in files+thread_handles:
        k.CloseHandle(h)
