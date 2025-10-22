import argparse
import time
import logging
import signal
from concurrent.futures import ProcessPoolExecutor, as_completed


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s] [%(processName)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

stop = False


def handle_signal(signum, frame):
    global stop
    logger.warning(f"Получен сигнал {signal.Signals(signum).name}, завершаю работу")
    stop = True


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def is_prime(num: int) -> bool:
    """Простая проверка числа на простоту"""  # Взял отсюда: https://stackoverflow.com/a/15285588
    if num == 2:
        return True
    if num % 2 == 0:
        return False
    for i in range(3, int(num**0.5) + 1, 2):
        if num % i == 0:
            return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=50000)
    args = parser.parse_args()

    numbers = args.limit
    max_workers = args.workers

    start = time.perf_counter()

    results = []
    logger.info(f"Запуск {args.workers} процессов")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(is_prime, n): n for n in range(2, numbers)}

        for future in as_completed(futures):
            number = futures[future]
            try:
                result = future.result(timeout=2)
                results.append((number, result))
            except Exception as err:
                logger.error(f"Ошибка при обработке {number}: {err}")

    end = time.perf_counter() - start
    throughput = len(results) / end

    logger.info(
        f"Завершено: {len(results)} чисел за {end:.2f} с "
        f"({throughput:.2f} чисел/с)"
    )


if __name__ == "__main__":
    main()