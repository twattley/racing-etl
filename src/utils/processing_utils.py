import concurrent.futures


def execute_stored_procedures(*funcs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(func) for func in funcs]
        for future in concurrent.futures.as_completed(futures):
            future.result()


def run_parallel_jobs(*func_args):
    with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
        for i in range(0, len(func_args), 2):
            futures = [
                executor.submit(func, *args) if args else executor.submit(func)
                for func, args in func_args[i : i + 2]
            ]
            concurrent.futures.wait(futures)
