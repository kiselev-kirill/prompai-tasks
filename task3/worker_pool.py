import multiprocessing as mp
import logging
import signal
import time
import os
from typing import Optional

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(processName)s: %(message)s",
    datefmt="%H:%M:%S",
)

def is_prime(n: int) -> bool:
    """Проверка, является ли число простым."""
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(n**0.5) + 1, 2):
        if n % i == 0:
            return False
    return True

def dispatcher(numbers: list[int], task_queue: mp.Queue, stop_marker: object, num_workers: int):
    """Кладет задания в task_queue и стоп-маркеры для каждого воркера."""
    for n in numbers:
        task_queue.put(n)
    logging.info("Dispatcher finished adding tasks")
    # Кладём по одному маркеру на каждого воркера
    for _ in range(num_workers):
        task_queue.put(stop_marker)

# --- Worker ---
def worker(task_queue: mp.Queue, result_queue: mp.Queue, stop_marker: object, read_timeout: float = 1.0):
    logging.info("Worker started")
    while True:
        try:
            task = task_queue.get(timeout=read_timeout)
        except Exception:
            continue  # таймаут, проверим снова

        if task is stop_marker:
            logging.info("Received stop marker. Exiting.")
            break

        n = task
        result = is_prime(n)
        result_queue.put((n, result))
    logging.info("Worker finished")
# --- Собираем результаты ---
def result_collector(result_queue: mp.Queue, stop_marker: object, total_tasks: int, read_timeout: float = 1.0):
    """
    Читает результаты из очереди и собирает статистику.
    """
    results = {}
    collected = 0
    start_time = time.time()

    while collected < total_tasks:
        try:
            n, is_prime_flag = result_queue.get(timeout=read_timeout)
            results[n] = is_prime_flag
            collected += 1
            if collected % 10 == 0 or collected == total_tasks:
                logging.info(f"Collected {collected}/{total_tasks} results")
        except Exception:
            continue

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Collected all {total_tasks} results in {duration:.2f} seconds")
    return results


def setup_signal_handlers(stop_marker: object, task_queue: mp.Queue):
    def handle_signal(signum, frame):
        logging.warning(f"Received signal {signum}. Sending stop marker to workers.")
        task_queue.put(stop_marker)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


def main(numbers: list[int], workers: Optional[int] = None):
    if workers is None:
        workers = int(os.environ.get("WORKERS", 4))

    logging.info(f"Starting with {workers} workers")

    task_queue: mp.Queue = mp.Queue()
    result_queue: mp.Queue = mp.Queue()
    stop_marker = object()  # уникальный маркер завершения

    setup_signal_handlers(stop_marker, task_queue)

    # Стартуем воркеры
    processes = []
    for i in range(workers):
        p = mp.Process(target=worker, args=(task_queue, result_queue, stop_marker))
        p.start()
        processes.append(p)

    # Запускаем диспетчер
    dispatcher(numbers, task_queue, stop_marker)

    # Собираем результаты
    results = result_collector(result_queue, stop_marker, total_tasks=len(numbers))

    # Ждём завершения воркеров
    for p in processes:
        p.join()
    logging.info("All workers have exited")

    # Простая телеметрия
    primes_count = sum(1 for v in results.values() if v)
    logging.info(f"Total primes found: {primes_count}")

    return results

# --- Пример запуска ---
if __name__ == "__main__":
    # Проверяем простые числа от 1 до 100
    nums = list(range(1, 101))
    main(nums, workers=4)