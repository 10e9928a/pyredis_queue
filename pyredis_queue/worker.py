# -*- coding: utf-8 -*-
"""
Worker模块

提供任务执行器的实现。
"""

from __future__ import annotations

import signal
import logging
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, Future
from types import FrameType
from typing import Callable

from pyredis_queue.connection import RedisConnection
from pyredis_queue.task import Task, TaskRegistry
from pyredis_queue.queue import TaskQueue

logger = logging.getLogger(__name__)


class Worker:
    """
    任务执行器

    负责从队列获取任务并执行。
    """

    def __init__(
        self,
        queue_name: str = 'default',
        concurrency: int = 4,
        connection: RedisConnection | None = None,
        poll_interval: int = 1
    ) -> None:
        """
        初始化Worker

        :param queue_name: 队列名称
        :param concurrency: 并发数
        :param connection: Redis连接实例
        :param poll_interval: 轮询间隔（秒）
        """
        self.queue_name: str = queue_name
        self.concurrency: int = concurrency
        self.poll_interval: int = poll_interval

        self._queue: TaskQueue = TaskQueue(
            queue_name=queue_name,
            connection=connection
        )
        self._executor: ThreadPoolExecutor | None = None
        self._running: bool = False
        self._shutdown_event: threading.Event = threading.Event()
        self._active_tasks: dict[str, Future[object]] = {}
        self._lock: threading.Lock = threading.Lock()

        # 回调函数
        self._on_task_start: Callable[[Task], None] | None = None
        self._on_task_success: Callable[[Task, object], None] | None = None
        self._on_task_failure: Callable[[Task, Exception], None] | None = None

    @property
    def queue(self) -> TaskQueue:
        """获取任务队列"""
        return self._queue

    def on_task_start(self, callback: Callable[[Task], None]) -> None:
        """设置任务开始回调"""
        self._on_task_start = callback

    def on_task_success(
        self,
        callback: Callable[[Task, object], None]
    ) -> None:
        """设置任务成功回调"""
        self._on_task_success = callback

    def on_task_failure(
        self,
        callback: Callable[[Task, Exception], None]
    ) -> None:
        """设置任务失败回调"""
        self._on_task_failure = callback

    def start(self, daemon: bool = False) -> None:
        """
        启动Worker

        :param daemon: 是否以守护模式运行
        """
        if self._running:
            logger.warning('Worker已经在运行中')
            return

        self._running = True
        self._shutdown_event.clear()
        self._executor = ThreadPoolExecutor(
            max_workers=self.concurrency,
            thread_name_prefix='task_worker'
        )

        # 注册信号处理器
        self._setup_signal_handlers()

        logger.info(
            f'Worker启动 - 队列: {self.queue_name}, 并发数: {self.concurrency}'
        )

        if daemon:
            # 守护模式，在新线程中运行
            thread = threading.Thread(
                target=self._run_loop,
                daemon=True
            )
            thread.start()
        else:
            # 阻塞模式
            self._run_loop()

    def stop(self, wait: bool = True) -> None:
        """
        停止Worker

        :param wait: 是否等待任务完成
        """
        if not self._running:
            return

        logger.info('正在停止Worker...')
        self._running = False
        self._shutdown_event.set()

        if self._executor is not None:
            self._executor.shutdown(wait=wait)
            self._executor = None

        logger.info('Worker已停止')

    def _setup_signal_handlers(self) -> None:
        """设置信号处理器"""
        def signal_handler(signum: int, frame: FrameType | None) -> None:
            _ = frame  # 标记为已使用
            logger.info(f'收到信号 {signum}，正在优雅关闭...')
            self.stop(wait=True)

        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        except ValueError:
            # 在非主线程中无法设置信号处理器
            pass

    def _run_loop(self) -> None:
        """主循环"""
        while self._running and not self._shutdown_event.is_set():
            try:
                # 检查是否有空闲的工作线程
                with self._lock:
                    active_count = len(self._active_tasks)

                if active_count >= self.concurrency:
                    # 所有线程都忙，等待一下
                    self._shutdown_event.wait(timeout=.1)
                    continue

                # 从队列获取任务
                task = self._queue.dequeue(timeout=self.poll_interval)

                if task is None:
                    continue

                if self._executor is None:
                    continue

                # 提交任务到线程池
                future: Future[object] = self._executor.submit(
                    self._execute_task, task
                )

                with self._lock:
                    self._active_tasks[task.task_id] = future

                # 添加完成回调
                future.add_done_callback(
                    lambda f, tid=task.task_id: self._on_task_done(tid, f)
                )

            except Exception as e:
                logger.error(f'Worker循环错误: {e}')
                self._shutdown_event.wait(timeout=1)

    def _execute_task(self, task: Task) -> object:
        """
        执行任务

        :param task: 任务对象
        :return: 执行结果
        """
        logger.info(f'开始执行任务: {task.task_id} ({task.name})')

        # 触发开始回调
        if self._on_task_start:
            try:
                self._on_task_start(task)
            except Exception as e:
                logger.warning(f'任务开始回调错误: {e}')

        # 获取任务处理器
        handler = TaskRegistry.get_handler(task.name)
        if handler is None:
            raise ValueError(f'未找到任务处理器: {task.name}')

        # 执行任务
        result = handler(**task.payload)

        return result

    def _on_task_done(self, task_id: str, future: Future[object]) -> None:
        """任务完成回调"""
        with self._lock:
            self._active_tasks.pop(task_id, None)

        task = self._queue.get_task(task_id)
        if task is None:
            return

        try:
            result = future.result()
            self._queue.complete(task, result)

            logger.info(f'任务完成: {task_id}')

            # 触发成功回调
            if self._on_task_success:
                try:
                    self._on_task_success(task, result)
                except Exception as e:
                    logger.warning(f'任务成功回调错误: {e}')

        except Exception as e:
            error_msg = f'{type(e).__name__}: {e!s}'
            logger.error(
                f'任务执行失败: {task_id}\n'
                f'{traceback.format_exc()}'
            )

            self._queue.fail(task, error_msg)

            # 触发失败回调
            if self._on_task_failure:
                try:
                    self._on_task_failure(task, e)
                except Exception as cb_error:
                    logger.warning(f'任务失败回调错误: {cb_error}')

    def get_active_tasks(self) -> list[str]:
        """获取正在执行的任务ID列表"""
        with self._lock:
            return list(self._active_tasks.keys())

    def is_running(self) -> bool:
        """检查Worker是否在运行"""
        return self._running


class WorkerPool:
    """
    Worker池

    管理多个Worker实例。
    """

    def __init__(
        self,
        queue_names: list[str],
        workers_per_queue: int = 1,
        concurrency: int = 4,
        connection: RedisConnection | None = None
    ) -> None:
        """
        初始化Worker池

        :param queue_names: 队列名称列表
        :param workers_per_queue: 每个队列的Worker数量
        :param concurrency: 每个Worker的并发数
        :param connection: Redis连接实例
        """
        self.queue_names: list[str] = queue_names
        self.workers_per_queue: int = workers_per_queue
        self.concurrency: int = concurrency
        self._connection: RedisConnection | None = connection
        self._workers: list[Worker] = []
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        """启动所有Worker"""
        for queue_name in self.queue_names:
            for i in range(self.workers_per_queue):
                worker = Worker(
                    queue_name=queue_name,
                    concurrency=self.concurrency,
                    connection=self._connection
                )
                self._workers.append(worker)

                thread = threading.Thread(
                    target=worker.start,
                    daemon=True,
                    name=f'worker-{queue_name}-{i}'
                )
                self._threads.append(thread)
                thread.start()

        logger.info(
            f'WorkerPool启动 - 队列数: {len(self.queue_names)}, '
            f'总Worker数: {len(self._workers)}'
        )

    def stop(self, wait: bool = True) -> None:
        """停止所有Worker"""
        for worker in self._workers:
            worker.stop(wait=wait)

        if wait:
            for thread in self._threads:
                thread.join(timeout=30)

        self._workers.clear()
        self._threads.clear()

        logger.info('WorkerPool已停止')

    def get_stats(self) -> dict[str, object]:
        """获取统计信息"""
        stats: dict[str, object] = {
            'total_workers': len(self._workers),
            'running_workers': sum(
                1 for w in self._workers if w.is_running()
            ),
            'queues': {},
        }

        queues_stats: dict[str, dict[str, int]] = {}
        for queue_name in self.queue_names:
            queue = TaskQueue(
                queue_name=queue_name,
                connection=self._connection
            )
            queues_stats[queue_name] = queue.get_queue_stats()

        stats['queues'] = queues_stats
        return stats
