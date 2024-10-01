import gzip
import bz2
import orjson
import sys
import asyncio
import aiofiles
import time
import psutil
from tqdm import tqdm
from multiprocessing import Pool, Queue, Event, Value

class WikidataDumpReader:
    def __init__(self, file_path, num_processes=4, batch_size=1000, queue_size=10000, skiplines=0):
        """
        Initializes the reader with the file path, number of processes for multiprocessing,
        and batch size for reading lines.
        """
        self.file_path = file_path
        self.extension = file_path.split(".")[-1]
        self.num_processes = num_processes
        self.batch_size = batch_size
        self.skiplines = skiplines
        self.queue = Queue(maxsize=queue_size)

        self.finished = Value('i', False)
        self.iterations = Value('i', 0)

    def lines_to_entities(self, lines):
        """
        Converts a single line of text into a Wikidata entity.
        """
        lines = lines.strip()
        if lines[-1] == ",":
            lines = lines[:-1]
        if lines[0] != "[":
            lines = '[' + lines
        if lines[-1] != "]":
            lines = lines + ']'

        entities = None
        try:
            entities = orjson.loads(lines)
        except ValueError as e:
            print("Failed to parse JSON", e)
            raise e

        return entities

    async def run(self, handler_func, max_iterations=None, verbose=True):
        """
        Asynchronously processes the input file. It reads the file in batches, converts lines
        to entities, and applies a handler function to each entity using a producer-consumer model.
        
        :param handler_func: A function to handle/process each entity.
        :param max_iterations: Max number of iterations (for testing purposes).
        :param verbose: If true, prints processing stats.
        """

        producer = asyncio.to_thread(self._producer, max_iterations)
        consumers = [
            asyncio.to_thread(self._consumer, handler_func) for _ in range(self.num_processes)
        ]

        tasks = [producer, *consumers]
        if verbose:
            reporter = asyncio.to_thread(self._reporter)
            tasks.append(reporter)

        await asyncio.gather(*tasks)

    def _reporter(self):
        """
        Reads lines from the file in batches and pushes individual entities into the queue.
        """
        start = time.time()

        while (not self.finished.value) or (not self.queue.empty()):
            time.sleep(3)
            
            time_per_iteration_s = time.time() - start
            lines_per_s = (self.iterations.value * self.batch_size) / time_per_iteration_s

            process = psutil.Process()
            memory_info = process.memory_info()
            memory_usage_mb = memory_info.rss / 1024 ** 2

            print(
                f"{(self.iterations.value * self.batch_size)} Lines Processed \t Line Process Avg: {lines_per_s:.0f} items/sec \t Memory Usage Avg: {memory_usage_mb:.2f} MB",
                file=sys.stderr,
            )
            

    def _producer(self, max_iterations):
        """
        Reads lines from the file in batches and pushes individual entities into the queue.
        """
        with self.finished.get_lock():
            self.finished.value = False

        iters = 0
        if self.extension == 'json':
            read_lines = self._read_jsonfile()
        elif self.extension in ['gz', 'bz2']:
            read_lines = self._read_zipfile()
        else:
            raise ValueError("File extension is not supported")

        for lines_batch in read_lines:
            self.queue.put(lines_batch)

            iters += 1
            if max_iterations and (iters >= max_iterations):
                break
            
        with self.finished.get_lock():
            self.finished.value = True

    def _consumer(self, handler_func):
        """
        Consumes JSON entities from the queue and processes them using the handler function.
        """
        while (not self.finished.value) or (not self.queue.empty()):
            lines_batch = None
            try:
                lines_batch = self.queue.get(timeout=1)
            except:
                if self.finished.value:
                    break

            if lines_batch:
                entities_batch = self.lines_to_entities(lines_batch)

                for entity in entities_batch:
                    if entity:
                        handler_func(entity)

                with self.iterations.get_lock():
                    self.iterations.value += 1

    def _read_jsonfile(self):
        """
        Asynchronously reads lines from a JSON file in batches.
        """
        file = None
        try:
            file = open(self.file_path, mode="r")
            for _ in tqdm(range(self.skiplines)):
                file.readline()

            while True:
                lines_batch = ''
                for _ in range(self.batch_size):
                    line = file.readline()
                    if not line:
                        break
                    lines_batch = lines_batch + line

                if lines_batch == '':
                    break
                yield lines_batch

        finally:
            if file:
                file.close()


    def _read_zipfile(self):
        """
        Asynchronously reads lines from a compressed file (gzip or bz2).
        """
        file = None
        try:
            if self.extension == 'gz':
                file = gzip.open(self.file_path, "rt")
            elif self.extension == 'bz2':
                file = bz2.open(self.file_path, "rt")
            else:
                raise ValueError("Zip file extension is not supported")

            for _ in tqdm(range(self.skiplines)):
                file.readline()

            while True:
                lines_batch = ''
                for _ in range(self.batch_size):
                    line = file.readline()
                    if not line:
                        break
                    lines_batch = lines_batch + line

                if lines_batch == '':
                    break
                yield lines_batch

        finally:
            if file:
                file.close()