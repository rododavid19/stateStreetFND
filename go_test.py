from ctypes import *
from ctypes import cdll


class go_string(Structure):
    _fields_ = [
        ("p", c_char_p),
        ("n", c_int)]


def goEval():
    lib = cdll.LoadLibrary('./libpy.so')
    lib.py.argtypes = [go_string]
    print("Loaded go generated SO library")
    message = go_string(b"Hell!", 6)
    result = lib.py(message)
    print(result)


if __name__ == '__main__':
    goEval()
