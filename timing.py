import time

def tik():
    return time.perf_counter()

def tok(start, out_str = None):
    t = time.perf_counter() - start
    if out_str:
        print(f"{out_str} {t:.03f} seconds")
    else:
        return t