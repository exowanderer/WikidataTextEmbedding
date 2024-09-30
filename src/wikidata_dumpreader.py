import gzip
import bz2
import orjson
import sys
import asyncio
import aiofiles
import time
import psutil

class WikidataDumpReader:
    def __init__(self, file_path, num_processes=4, batch_size=1000):
        """
        Initializes the reader with the file path, number of processes for multiprocessing,
        and batch size for reading lines.
        """
        self.file_path = file_path
        self.extension = file_path.split(".")[-1]
        self.num_processes = num_processes
        self.batch_size = batch_size
        self.queue = asyncio.Queue(maxsize=batch_size)

        self.finished_lock = asyncio.Lock()
        self.finished = False

        self.iteration_lock = asyncio.Lock()
        self.iteration_event = asyncio.Event()
        self.iterations = 0

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
        
        producer = asyncio.create_task(self._producer(max_iterations))
        consumers = [
            asyncio.create_task(self._consumer(handler_func, verbose))
            for _ in range(self.num_processes)
        ]
        tasks = [producer, *consumers]
        if verbose:
            reporter = asyncio.create_task(self._reporter())
            tasks.append(reporter)

        await asyncio.gather(*tasks)

    async def _reporter(self):
        """
        Reads lines from the file in batches and pushes individual entities into the queue.
        """
        start = time.time()
        line_per_s_values = []
        mem_usage_values = []
        total_iterations = 0

        while (not self.finished) or (not self.queue.empty()):
            await self.iteration_event.wait()
            
            total_iterations += self.iterations
            time_per_iteration_s = time.time() - start
            lines_per_s = self.iterations / time_per_iteration_s
            line_per_s_values.append(lines_per_s)
            line_per_s_values = line_per_s_values[-100:]
            lines_per_s_avg = sum(line_per_s_values) / len(line_per_s_values)
            start = time.time()

            process = psutil.Process()
            memory_info = process.memory_info()
            memory_usage_mb = memory_info.rss / 1024 ** 2
            mem_usage_values.append(memory_usage_mb)
            mem_usage_values = mem_usage_values[-100:]
            mem_usage_avg = sum(mem_usage_values) / len(mem_usage_values)

            async with self.iteration_lock:
                self.iterations = 0
                self.iteration_event.clear()

            print(
                f"{total_iterations} Lines Processed \t Line Process Avg: {lines_per_s_avg:.0f} items/sec \t Memory Usage Avg: {mem_usage_avg:.2f} MB",
                file=sys.stderr,
            )
            

    async def _producer(self, max_iterations):
        """
        Reads lines from the file in batches and pushes individual entities into the queue.
        """
        async with self.finished_lock:
            self.finished = False

        iters = 0
        if self.extension == 'json':
            read_lines = self._read_jsonfile()
        elif self.extension in ['gz', 'bz2']:
            read_lines = self._read_zipfile()
        else:
            raise ValueError("File extension is not supported")

        for lines_batch in read_lines:
            entities_batch = self.lines_to_entities(lines_batch)

            for entity in entities_batch:
                if entity:
                    await self.queue.put(entity)

            iters += 1
            if max_iterations and (iters >= max_iterations):
                break
            
        async with self.finished_lock:
            self.finished = True
            self.iteration_event.set()

    async def _consumer(self, handler_func, verbose):
        """
        Consumes JSON entities from the queue and processes them using the handler function.
        """
        while (not self.finished) or (not self.queue.empty()):
            entity = None
            try:
                entity = await asyncio.wait_for(self.queue.get(), timeout=1)
            except asyncio.TimeoutError:
                if self.finished:
                    break

            if entity:
                await handler_func(entity)

                async with self.iteration_lock:
                    if self.iterations >= self.batch_size:
                        self.iteration_event.set()
                    self.iterations += 1

    def _read_jsonfile(self):
        """
        Asynchronously reads lines from a JSON file in batches.
        """
        file = None
        try:
            file = open(self.file_path, mode="r")

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