
def progress(total,current):
    done = int(50 * current / total)
    print("[%s%s]" % ('=' * done, ' ' * (50-done)), end="\r", flush=True )