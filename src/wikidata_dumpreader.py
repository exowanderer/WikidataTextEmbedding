import gzip
import bz2
import orjson
import time
import psutil
from tqdm import tqdm
from multiprocessing import Process, Queue, Value

class WikidataDumpReader:
    def __init__(self, file_path, num_processes=4, queue_size=1000, skiplines=0):
        """
        Initializes the reader with the file path, number of processes, queue size, and number of lines to skip.

        Parameters:
        - file_path (str): Path to the dump file.
        - num_processes (int): Number of consumer processes to spawn (default=4).
        - queue_size (int): Maximum size of the queue (default=1000).
        - skiplines (int): Number of lines to skip at the beginning of the file (default=0).
        """
        self.file_path = file_path
        self.extension = file_path.split(".")[-1]
        self.num_processes = num_processes
        self.skiplines = skiplines

        # This queue is shared across all processes
        self.queue = Queue(maxsize=queue_size)

        # Shared multiprocessing values:
        # - finished: 0 => not finished, 1 => finished
        # - iterations: a counter for how many entities have been processed
        self.finished = Value('i', 0)
        self.iterations = Value('i', 0)

    def line_to_entity(self, line):
        """
        Converts a single line of text into a Wikidata entity (a dictionary).

        Parameters:
        - line (str): A single line representing a Wikidata entity in JSON format.

        Returns:
        - dict or None: The parsed entity if valid JSON, or None if empty or malformed.
        """
        line = line.strip("[] ,\n")
        if not line:
            return None

        try:
            entity = orjson.loads(line)
            return entity
        except ValueError as e:
            # You can either log and continue or re-raise
            print("Failed to parse JSON:", e)
            return None

    def run(self, handler_func, max_iterations=None, verbose=True):
        """
        Starts processing using a producer-consumer model with multiprocessing.

        Spawns:
          - 1 Producer process (reads lines from file, pushes to queue).
          - N Consumer processes (parse lines, call handler_func).
          - 1 Reporter process (optional) that prints stats periodically.

        Parameters:
        - handler_func (callable): A function that takes a parsed entity (dict) as input.
        - max_iterations (int or None): Stop after this many lines (if not None).
        - verbose (bool): If True, spawns a reporter process to print stats.
        """
        # Create processes
        producer_p = Process(target=self._producer, args=(max_iterations,))
        consumer_ps = [
            Process(target=self._consumer, args=(handler_func,))
            for _ in range(self.num_processes)
        ]

        # Optional reporter
        reporter_p = None
        if verbose:
            reporter_p = Process(target=self._reporter, args=([producer_p] + consumer_ps,))

        # Start processes
        producer_p.start()
        for cp in consumer_ps:
            cp.start()
        if reporter_p:
            reporter_p.start()

        # Wait for processes to finish
        producer_p.join()
        for cp in consumer_ps:
            cp.join()
        if reporter_p:
            reporter_p.join()

    def _reporter(self):
        """
        Runs in its own process: reports overall progress and total memory usage every few seconds
        until the producer has finished and the queue is empty.

        Parameters
        ----------
        processes : list of multiprocessing.Process
            The list of processes (producer + consumers + possibly others) to track.
        """

        start_time = time.time()

        while True:
            time.sleep(3)

            # Grab iteration count
            with self.iterations.get_lock():
                items_processed = self.iterations.value

            # If finished and queue empty, exit
            if self.finished.value == 1 and self.queue.empty():
                break

            elapsed = time.time() - start_time
            rate = items_processed / elapsed if elapsed > 0 else 0.0

            print(
                f"Items Processed: {items_processed} | "
                f"Processing Rate: {rate:.0f} items/sec"
            )


    def _producer(self, max_iterations):
        """
        Reads lines from the file (plain or compressed) and puts them into the queue.
        Once done (or max_iterations reached), marks 'finished' as 1.
        """
        with self.finished.get_lock():
            self.finished.value = 0  # not finished

        iters = 0
        if self.extension == 'json':
            lines_gen = self._read_jsonfile()
        elif self.extension in ['gz', 'bz2']:
            lines_gen = self._read_zipfile()
        else:
            raise ValueError(f"File extension '{self.extension}' is not supported")

        for line in lines_gen:
            self.queue.put(line)  # Blocks if the queue is full
            iters += 1
            if max_iterations and iters >= max_iterations:
                break

        # Mark as finished
        with self.finished.get_lock():
            self.finished.value = 1

    def _consumer(self, handler_func):
        """
        Consumes lines from the queue, parses JSON, then invokes handler_func with the entity.
        Exits when 'finished' is set and the queue is empty.
        """
        while True:
            # If we are finished and the queue is empty, exit
            if self.finished.value == 1 and self.queue.empty():
                break

            try:
                line = self.queue.get(timeout=1)
            except Exception:
                # Usually queue.Empty, can wait for more data unless finished
                continue

            if line:
                entity = self.line_to_entity(line)
                if entity is not None:
                    handler_func(entity)

                with self.iterations.get_lock():
                    self.iterations.value += 1

    def _read_jsonfile(self):
        """
        Yields lines from a .json file, skipping self.skiplines lines at the start.
        """
        file = None
        try:
            file = open(self.file_path, mode="r", encoding="utf-8")
            # Skip lines if requested
            for _ in tqdm(range(self.skiplines), desc="Skipping lines"):
                file.readline()

            for line in file:
                if not line:
                    break
                yield line
        finally:
            if file:
                file.close()

    def _read_zipfile(self):
        """
        Yields lines from a .gz or .bz2 file, skipping self.skiplines lines at the start.
        """
        file = None
        try:
            if self.extension == 'gz':
                file = gzip.open(self.file_path, mode="rt", encoding="utf-8")
            elif self.extension == 'bz2':
                file = bz2.open(self.file_path, mode="rt", encoding="utf-8")
            else:
                raise ValueError(f"Unsupported extension '{self.extension}'")

            for _ in tqdm(range(self.skiplines), desc="Skipping lines"):
                file.readline()

            for line in file:
                if not line:
                    break
                yield line
        finally:
            if file:
                file.close()
