from multiprocessing import Pool
import gzip
import bz2
import json
import sys
import asyncio
import aiofiles
import time

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


    def line_to_entity(self, line):
        """
        Converts a single line of text into a Wikidata entity.
        """
        line = line.strip("\n").rstrip(",")
        if line == "[" or line == "]":
            return

        entity = None
        try:
            entity = json.loads(line)
        except ValueError as e:
            print("Failed to parse json", e, line)
            raise e

        return entity
    
    async def _handle_lines(self, pool, lines_batch):
        """
        Asynchronously handles a batch of lines by passing them to the multiprocessing pool,
        where each line is processed using the `line_to_entity` method.
        """
        return pool.map(self.line_to_entity, lines_batch)

    async def run(self, handler_func, max_iterations=None, verbose=True):
        """
        Asynchronously processes the input file. It reads the file in batches, converts lines
        to entities using multiple processes, and applies a handler function to each entity.
        
        :param handler_func: A function to handle/process each entity.
        :param max_iterations: Max number of iterations (for testing purposes).
        :param verbose: If true, prints processing stats.
        """
        with Pool(self.num_processes) as pool:
            start = time.time()
            line_per_ms_values = []
            iterations = 0

            if self.extension == 'json':
                read_lines = self._read_jsonfile()
            elif self.extension in ['gz', 'bz2']:
                read_lines = self._read_zipfile()
            else:
                raise ValueError("File extension is not supported")

            async for lines_batch in read_lines:
                if max_iterations is not None and iterations >= max_iterations:
                    break

                process_task = asyncio.create_task(
                    self._handle_lines(pool, lines_batch)
                )
                entities_batch = await process_task

                if handler_func:
                    for entity in entities_batch:
                        handler_func(entity)

                if verbose:
                    time_per_iteration_ms = (time.time() - start) * 1000
                    lines_per_ms = len(lines_batch) / time_per_iteration_ms
                    line_per_ms_values.append(lines_per_ms)
                    line_per_ms_values = line_per_ms_values[-16:]
                    lines_per_ms_avg = sum(line_per_ms_values) / len(line_per_ms_values)
                    iterations += 1
                    start = time.time()
                    print(
                        f"{iterations:5}: {lines_per_ms:.2f} (avg {lines_per_ms_avg:.2f}) ents/ms",
                        file=sys.stderr,
                    )
    
    async def _read_jsonfile(self):
        """
        Asynchronously reads lines from a JSON file in batches.
        """
        file = None
        try:
            file = await aiofiles.open(self.file_path, mode="r")
            read_task = asyncio.create_task(file.readlines(self.batch_size))

            while True:
                lines_batch = await read_task
                if not lines_batch:
                    break

                read_task = asyncio.create_task(file.readlines(self.batch_size))
                yield lines_batch

        finally:
            if file:
                await file.close()

    
    async def _read_zipfile(self, chunk_size=1024*1024):
        """
        Asynchronously reads lines from a compressed file (gzip or bz2).
        """
        file = None
        if self.extension == 'gz':
            file = await asyncio.to_thread(gzip.open, self.file_path, "rt")
        elif self.extension == 'bz2':
            file = await asyncio.to_thread(bz2.open, self.file_path, "rt")
        else:
            raise ValueError("Zip File extension is not supported")

        lines_batch = []
        buffer = ""
        try:
            while True:
                data = await asyncio.to_thread(file.read, chunk_size)
                if not data:
                    if buffer:
                        lines_batch.append(buffer)
                    if lines_batch:
                        yield lines_batch
                    break

                buffer += str(data)
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    lines_batch.append(line)

                    if len(lines_batch) >= self.batch_size:
                        yield lines_batch[:self.batch_size]
                        lines_batch = lines_batch[self.batch_size:]
                    
            if buffer:
                lines_batch.append(buffer)
            if lines_batch:
                yield lines_batch
        
        finally:
            await asyncio.to_thread(file.close)