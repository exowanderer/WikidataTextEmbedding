# from multiprocessing import Pool
# import gzip
# import bz2
# import orjson
# import sys
# import asyncio
# import aiofiles
# import time
# import psutil

# class WikidataDumpReader:

#     def __init__(self, file_path, num_processes=4, batch_size=1000):
#         """
#         Initializes the reader with the file path, number of processes for multiprocessing,
#         and batch size for reading lines.
#         """
#         self.file_path = file_path
#         self.extension = file_path.split(".")[-1]
#         self.num_processes = num_processes
#         self.batch_size = batch_size


#     def line_to_entity(self, line):
#         """
#         Converts a single line of text into a Wikidata entity.
#         """
#         line = line.strip("\n").rstrip(",")
#         if line == "[" or line == "]":
#             return

#         entity = None
#         try:
#             entity = orjson.loads(line)
#         except ValueError as e:
#             print("Failed to parse json", e, line)
#             raise e

#         return entity
    
#     async def _handle_lines(self, pool, lines_batch):
#         """
#         Asynchronously handles a batch of lines by passing them to the multiprocessing pool,
#         where each line is processed using the `line_to_entity` method.
#         """
#         return pool.map(self.line_to_entity, lines_batch)

#     async def run(self, handler_func, max_iterations=None, verbose=True):
#         """
#         Asynchronously processes the input file. It reads the file in batches, converts lines
#         to entities using multiple processes, and applies a handler function to each entity.
        
#         :param handler_func: A function to handle/process each entity.
#         :param max_iterations: Max number of iterations (for testing purposes).
#         :param verbose: If true, prints processing stats.
#         """
#         with Pool(self.num_processes) as pool:
#             start = time.time()
#             line_per_s_values = []
#             iterations = 0

#             if self.extension == 'json':
#                 read_lines = self._read_jsonfile()
#             elif self.extension in ['gz', 'bz2']:
#                 read_lines = self._read_zipfile()
#             else:
#                 raise ValueError("File extension is not supported")

#             async for lines_batch in read_lines:
#                 if (max_iterations is not None) and (iterations >= max_iterations):
#                     break

#                 process_task = asyncio.create_task(
#                     self._handle_lines(pool, lines_batch)
#                 )
#                 entities_batch = await process_task

#                 if handler_func:
#                     for entity in entities_batch:
#                         handler_func(entity)

#                 if verbose:
#                     time_per_iteration_s = (time.time() - start)
#                     lines_per_s = len(lines_batch) / time_per_iteration_s
#                     line_per_s_values.append(lines_per_s)
#                     line_per_s_values = line_per_s_values[-1000:]
#                     lines_per_s_avg = sum(line_per_s_values) / len(line_per_s_values)
#                     iterations += 1
#                     start = time.time()

#                     process = psutil.Process()
#                     memory_info = process.memory_info()
#                     memory_usage_mb = memory_info.rss / 1024 ** 2

#                     print(
#                         f"{iterations:5}: {lines_per_s:.0f} (avg {lines_per_s_avg:.0f}) items/sec \t Memory Usage: {memory_usage_mb:.2f} MB",
#                         file=sys.stderr,
#                     )
    
#     async def _read_jsonfile(self):
#         """
#         Asynchronously reads lines from a JSON file in batches.
#         """
#         file = None
#         try:
#             file = await aiofiles.open(self.file_path, mode="r")

#             while True:
#                 lines_batch = []
#                 for _ in range(self.batch_size):
#                     line = await file.readline()
#                     if not line:
#                         break
#                     lines_batch.append(line)

#                 if not lines_batch:
#                     break
#                 yield lines_batch

#         finally:
#             if file:
#                 await file.close()

    
#     async def _read_zipfile(self):
#         """
#         Asynchronously reads lines from a compressed file (gzip or bz2).
#         """
#         file = None
#         try:
#             if self.extension == 'gz':
#                 file = await asyncio.to_thread(gzip.open, self.file_path, "rt")
#             elif self.extension == 'bz2':
#                 file = await asyncio.to_thread(bz2.open, self.file_path, "rt")
#             else:
#                 raise ValueError("Zip File extension is not supported")

#             while True:
#                 lines_batch = []
#                 for _ in range(self.batch_size):
#                     line = await asyncio.to_thread(file.readline)
#                     if not line:
#                         break
#                     lines_batch.append(line)

#                 if not lines_batch:
#                     break
#                 yield lines_batch

#         finally:
#             if file:
#                 await asyncio.to_thread(file.close)


from multiprocessing import Pool
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


    def lines_to_entities(self, lines):
        """
        Converts a single line of text into a Wikidata entity.
        """
        lines = lines.strip()
        if lines[-1] == ",":
            lines = lines[:-1]
        if lines[0] != "[":
            lines = '['+lines
        if lines[-1] != "]":
            lines = lines+']'

        entities = None
        try:
            entities = orjson.loads(lines)
        except ValueError as e:
            print("Failed to parse json", e)
            raise e

        return entities
    
    # async def _handle_lines(self, pool, lines_batch):
    #     """
    #     Asynchronously handles a batch of lines by passing them to the multiprocessing pool,
    #     where each line is processed using the `line_to_entity` method.
    #     """
    #     return pool.map(self.line_to_entity, lines_batch)

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
            line_per_s_values = []
            iterations = 0

            if self.extension == 'json':
                read_lines = self._read_jsonfile()
            elif self.extension in ['gz', 'bz2']:
                read_lines = self._read_zipfile()
            else:
                raise ValueError("File extension is not supported")

            async for lines_batch in read_lines:
                if (max_iterations is not None) and (iterations >= max_iterations):
                    break

                entities_batch = await asyncio.to_thread(self.lines_to_entities, lines_batch)

                if handler_func:
                    for entity in entities_batch:
                        handler_func(entity)

                if verbose:
                    time_per_iteration_s = (time.time() - start)
                    lines_per_s = len(entities_batch) / time_per_iteration_s
                    line_per_s_values.append(lines_per_s)
                    line_per_s_values = line_per_s_values[-1000:]
                    lines_per_s_avg = sum(line_per_s_values) / len(line_per_s_values)
                    iterations += 1
                    start = time.time()

                    process = psutil.Process()
                    memory_info = process.memory_info()
                    memory_usage_mb = memory_info.rss / 1024 ** 2

                    print(
                        f"{iterations:5}: {lines_per_s:.0f} (avg {lines_per_s_avg:.0f}) items/sec \t Memory Usage: {memory_usage_mb:.2f} MB",
                        file=sys.stderr,
                    )
    
    async def _read_jsonfile(self):
        """
        Asynchronously reads lines from a JSON file in batches.
        """
        file = None
        try:
            file = await aiofiles.open(self.file_path, mode="r")

            while True:
                lines_batch = ''
                for _ in range(self.batch_size):
                    line = await file.readline()
                    if not line:
                        break
                    lines_batch = lines_batch+line

                if lines_batch == '':
                    break
                yield lines_batch

        finally:
            if file:
                await file.close()

    
    async def _read_zipfile(self):
        """
        Asynchronously reads lines from a compressed file (gzip or bz2).
        """
        file = None
        try:
            if self.extension == 'gz':
                file = await asyncio.to_thread(gzip.open, self.file_path, "rt")
            elif self.extension == 'bz2':
                file = await asyncio.to_thread(bz2.open, self.file_path, "rt")
            else:
                raise ValueError("Zip File extension is not supported")

            buffer = ''
            lines_batch = []
            while True:
                data = await asyncio.to_thread(file.read, 1024*1024*100)
                if not data:
                    break
                buffer += data
                lines = buffer.split('\n')
                buffer = lines.pop()

                for line in lines:
                    lines_batch.append(line)
                    if len(lines_batch) >= self.batch_size:
                        yield '\n'.join(lines_batch)
                        lines_batch = []

            if buffer:
                lines_batch.append(buffer)
            if lines_batch:
                yield '\n'.join(lines_batch)

        finally:
            if file:
                await asyncio.to_thread(file.close)