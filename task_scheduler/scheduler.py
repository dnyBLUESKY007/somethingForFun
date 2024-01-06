from datetime import datetime, timedelta
import asyncio
import logging

class Task:
    def __init__(self, *args, desc: str = '') -> None:
        self.args = args
        self.desc = desc

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.desc})"
        
    def run(self) -> None:
        raise NotImplementedError(f"{self}.run(): Not implemented.")
    
class TaskAsync:
    def __init__(self, *args, desc: str = '') -> None:
        self.args = args
        self.desc = desc

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.desc})"
        
    async def run(self) -> None:
        raise NotImplementedError(f"{self}.run(): Not implemented.")


class Scheduler:
    TIME_EPS = 0.5
    @classmethod
    async def run_after(self, task: Task, after_date_time: datetime):
        logging.info(f"Scheduler.run_after({task}): task will start at {after_date_time}.")
        try:
            while datetime.now() < after_date_time:
                await asyncio.sleep(self.TIME_EPS)
            start_time = datetime.now()
            logging.info(f"Scheduler.run_after({task}): start at {start_time}.")
            return task.run()
        except BaseException as e:
            logging.error(f"Scheduler.run_after({task}): failed to run the task. {e}")
        finally:
            end_time = datetime.now()
            logging.info(f"Scheduler.run_after({task}): end at {end_time}, time used ({end_time - start_time}).")
            
    @classmethod
    async def run_async_after(self, task: TaskAsync, after_date_time: datetime):
        logging.info(f"Scheduler.run_async_after({task}): task will start at {after_date_time}.")
        try:
            while datetime.now() < after_date_time:
                await asyncio.sleep(self.TIME_EPS)
            start_time = datetime.now()
            logging.info(f"Scheduler.run_async_after({task}): start at {start_time}.")
            return await task.run()
        except BaseException as e:
            logging.error(f"Scheduler.run_async_after({task}): failed to run the task. {e}")
        finally:
            end_time = datetime.now()
            logging.info(f"Scheduler.run_async_after({task}): end at {end_time}, time used ({end_time - start_time}).")

    @classmethod
    async def run_periodically_since(self, task: Task, since_date_time: datetime, period: timedelta):
        if not period >= timedelta(seconds=1):
            logging.error(f"Scheduler.run_periodically_since({task}): period too short ({timedelta.seconds} < 1s)")
        next_run_date_time = since_date_time
        logging.info(f"Scheduler.run_periodically_since({task}): task will run periodically since {next_run_date_time}.")
        while True:
            try:
                while datetime.now() < next_run_date_time:
                    await asyncio.sleep(self.TIME_EPS)
                start_time = datetime.now()
                logging.info(f"Scheduler.run_periodically_since({task}): start at {start_time}.")
                task.run()
                while next_run_date_time < datetime.now():
                    next_run_date_time += period
                end_time = datetime.now()
                logging.info(f"Scheduler.run_periodically_since({task}): end at {end_time}, time used ({end_time - start_time}).")
                logging.info(f"Scheduler.run_periodically_since({task}): next turn will start at {next_run_date_time}.")
            except BaseException as e:
                logging.error(f"Scheduler.run_periodically_since({task}): failed to run the task. {e}")
                break

    @classmethod
    async def run_async_periodically_since(self, task: TaskAsync, since_date_time: datetime, period: timedelta):
        if not period >= timedelta(seconds=1):
            logging.error(f"Scheduler.run_async_periodically_since({task}): period too short ({timedelta.seconds} < 1s)")
        next_run_date_time = since_date_time
        logging.info(f"Scheduler.run_async_periodically_since({task}): task will run periodically since {next_run_date_time}.")
        while True:
            try:
                while datetime.now() < next_run_date_time:
                    await asyncio.sleep(self.TIME_EPS)
                start_time = datetime.now()
                logging.info(f"Scheduler.run_async_periodically_since({task}): start at {start_time}.")
                await task.run()
                while next_run_date_time < datetime.now():
                    next_run_date_time += period
                end_time = datetime.now()
                logging.info(f"Scheduler.run_async_periodically_since({task}): end at {end_time}, time used ({end_time - start_time}).")
                logging.info(f"Scheduler.run_async_periodically_since({task}): next turn will start at {next_run_date_time}.")
            except BaseException as e:
                logging.error(f"Scheduler.run_async_periodically_since({task}): failed to run the task. {e}")
                break
