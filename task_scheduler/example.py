from scheduler import Task, Scheduler
from datetime import datetime, timedelta
import logging
import asyncio

class TaskExample(Task):
    def run(self) -> None:
        print(f"{self}.args:", self.args)
        s = 0
        for i in self.args:
            s += i
        print(f"{self}:", s)

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s | %(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    start_time = datetime.now()
    tasks = [
        Scheduler.run_after(TaskExample(0, 1, desc='task_5'), start_time+timedelta(seconds=1)), 
        Scheduler.run_after(TaskExample(1, 2, desc='task_1'), start_time+timedelta(seconds=2)), 
        Scheduler.run_after(TaskExample(3, 4, desc='task_2'), start_time+timedelta(seconds=3)), 
        Scheduler.run_after(TaskExample(5, 6, desc='task_3'), start_time+timedelta(seconds=4)), 
        Scheduler.run_after(TaskExample(7, 8, desc='task_4'), datetime(year=2024, month=1, day=3, hour=17, minute=52, second=15)), 
        Scheduler.run_periodically_since(TaskExample(1, 2, desc='task_a1'), start_time+timedelta(seconds=2), period=timedelta(seconds=3)), 
        Scheduler.run_periodically_since(TaskExample(3, 4, desc='task_a2'), start_time+timedelta(seconds=3), period=timedelta(seconds=4)), 
        Scheduler.run_periodically_since(TaskExample(5, 6, desc='task_a3'), start_time+timedelta(seconds=4), period=timedelta(seconds=5)), 
        Scheduler.run_periodically_since(TaskExample(7, 8, desc='task_a4'), datetime(year=2024, month=1, day=3, hour=17, minute=52, second=15), period=timedelta(seconds=2)), 
    ]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
