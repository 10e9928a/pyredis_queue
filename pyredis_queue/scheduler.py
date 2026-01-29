# -*- coding: utf-8 -*-
"""
任务调度器模块

提供定时任务、周期性任务的调度功能。
"""

from __future__ import annotations

import time
import logging
import threading
from redis import Redis
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Callable

from pyredis_queue.connection import RedisConnection
from pyredis_queue.task import Task, TaskPriority
from pyredis_queue.queue import TaskQueue




logger = logging.getLogger(__name__)


@dataclass
class ScheduledJob:
    """
    定时任务配置

    :param name: 任务名称
    :param task_name: 对应的Task名称
    :param payload: 任务参数
    :param interval: 执行间隔（秒）
    :param cron: Cron表达式（简化版）
    :param run_immediately: 是否立即执行一次
    :param priority: 任务优先级
    :param max_instances: 最大并发实例数
    :param enabled: 是否启用
    """
    name: str
    task_name: str
    payload: dict[str, object] = field(default_factory=dict)
    interval: int | None = None
    cron: str | None = None
    run_immediately: bool = False
    priority: int = TaskPriority.NORMAL
    max_instances: int = 1
    enabled: bool = True

    # 内部状态
    last_run: float | None = None
    next_run: float | None = None
    running_count: int = 0


class SimpleCronParser:
    """简化的Cron表达式解析器"""

    @staticmethod
    def parse(cron_expr: str) -> dict[str, list[int] | None]:
        """
        解析Cron表达式

        格式：分 时 日 月 周
        支持：* 表示任意，数字表示具体值，逗号分隔多个值

        :param cron_expr: Cron表达式
        :return: 解析结果
        """
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            raise ValueError(f'无效的Cron表达式: {cron_expr}')

        return {
            'minute': SimpleCronParser._parse_field(parts[0], 0, 59),
            'hour': SimpleCronParser._parse_field(parts[1], 0, 23),
            'day': SimpleCronParser._parse_field(parts[2], 1, 31),
            'month': SimpleCronParser._parse_field(parts[3], 1, 12),
            'weekday': SimpleCronParser._parse_field(parts[4], 0, 6),
        }

    @staticmethod
    def _parse_field(
        field_value: str,
        min_val: int,
        max_val: int
    ) -> list[int] | None:
        """解析单个字段"""
        if field_value == '*':
            return None  # 表示任意值

        values: list[int] = []
        for part in field_value.split(','):
            if '-' in part:
                # 范围
                start, end = part.split('-')
                values.extend(range(int(start), int(end) + 1))
            elif '/' in part:
                # 步进
                _, step = part.split('/')
                values.extend(
                    v for v in range(min_val, max_val + 1)
                    if (v - min_val) % int(step) == 0
                )
            else:
                values.append(int(part))

        return sorted(set(values))

    @staticmethod
    def get_next_run(
        cron_expr: str,
        from_time: datetime | None = None
    ) -> datetime:
        """
        计算下次执行时间

        :param cron_expr: Cron表达式
        :param from_time: 起始时间
        :return: 下次执行时间
        """
        if from_time is None:
            from_time = datetime.now()

        parsed = SimpleCronParser.parse(cron_expr)

        # 从下一分钟开始查找
        current = from_time.replace(second=0, microsecond=0)
        current += timedelta(minutes=1)

        # 最多查找一年
        max_iterations = 365 * 24 * 60
        for _ in range(max_iterations):
            if SimpleCronParser._matches(current, parsed):
                return current
            current += timedelta(minutes=1)

        raise ValueError('无法计算下次执行时间')

    @staticmethod
    def _matches(dt: datetime, parsed: dict[str, list[int] | None]) -> bool:
        """检查时间是否匹配"""
        minute_vals = parsed['minute']
        if minute_vals is not None:
            if dt.minute not in minute_vals:
                return False

        hour_vals = parsed['hour']
        if hour_vals is not None:
            if dt.hour not in hour_vals:
                return False

        day_vals = parsed['day']
        if day_vals is not None:
            if dt.day not in day_vals:
                return False

        month_vals = parsed['month']
        if month_vals is not None:
            if dt.month not in month_vals:
                return False

        weekday_vals = parsed['weekday']
        if weekday_vals is not None:
            if dt.weekday() not in weekday_vals:
                return False

        return True


class TaskScheduler:
    """
    任务调度器

    支持定时任务和周期性任务的调度。
    """

    # Redis键前缀
    KEY_PREFIX: str = 'rtq:scheduler'

    def __init__(
        self,
        queue_name: str = 'default',
        connection: RedisConnection | None = None,
        check_interval: int = 1
    ) -> None:
        """
        初始化调度器

        :param queue_name: 默认队列名称
        :param connection: Redis连接实例
        :param check_interval: 检查间隔（秒）
        """
        self.queue_name: str = queue_name
        self.check_interval: int = check_interval
        self._connection: RedisConnection | None = connection

        self._queue: TaskQueue = TaskQueue(
            queue_name=queue_name,
            connection=connection
        )

        self._jobs: dict[str, ScheduledJob] = {}
        self._running: bool = False
        self._shutdown_event: threading.Event = threading.Event()
        self._lock: threading.Lock = threading.Lock()

        # Redis键
        self._lock_key: str = f'{self.KEY_PREFIX}:lock'
        self._jobs_key: str = f'{self.KEY_PREFIX}:jobs'

    @property
    def redis(self) -> Redis:  # type: ignore[type-arg]
        """获取Redis客户端"""
        if self._connection is not None:
            return self._connection.client
        return RedisConnection.get_client()

    def add_job(self, job: ScheduledJob) -> None:
        """
        添加定时任务

        :param job: 定时任务配置
        """
        with self._lock:
            # 计算下次执行时间
            if job.interval is not None:
                if job.run_immediately:
                    job.next_run = time.time()
                else:
                    job.next_run = time.time() + job.interval
            elif job.cron is not None:
                next_dt = SimpleCronParser.get_next_run(job.cron)
                job.next_run = next_dt.timestamp()

            self._jobs[job.name] = job
            next_run_time = job.next_run if job.next_run else time.time()
            logger.info(
                f'添加定时任务: {job.name}, ' +
                f'下次执行: {datetime.fromtimestamp(next_run_time)}'
            )

    def remove_job(self, job_name: str) -> bool:
        """
        移除定时任务

        :param job_name: 任务名称
        :return: 是否成功移除
        """
        with self._lock:
            if job_name in self._jobs:
                del self._jobs[job_name]
                logger.info(f'移除定时任务: {job_name}')
                return True
            return False

    def enable_job(self, job_name: str) -> bool:
        """启用任务"""
        with self._lock:
            if job_name in self._jobs:
                self._jobs[job_name].enabled = True
                return True
            return False

    def disable_job(self, job_name: str) -> bool:
        """禁用任务"""
        with self._lock:
            if job_name in self._jobs:
                self._jobs[job_name].enabled = False
                return True
            return False

    def schedule(
        self,
        task_name: str,
        interval: int | None = None,
        cron: str | None = None,
        run_immediately: bool = False,
        **kwargs: object
    ) -> Callable[[Callable[..., object]], Callable[..., object]]:
        """
        装饰器：注册定时任务

        :param task_name: 任务名称
        :param interval: 执行间隔（秒）
        :param cron: Cron表达式
        :param run_immediately: 是否立即执行
        :param kwargs: 其他参数
        :return: 装饰器函数
        """
        def decorator(func: Callable[..., object]) -> Callable[..., object]:
            # 注册任务处理器
            from pyredis_queue.task import TaskRegistry
            _ = TaskRegistry.register(task_name)(func)

            # 创建定时任务
            job = ScheduledJob(
                name=f'scheduled_{task_name}',
                task_name=task_name,
                interval=interval,
                cron=cron,
                run_immediately=run_immediately,
                payload=dict(kwargs) if kwargs else {},  # type: ignore[arg-type]
            )
            self.add_job(job)

            return func
        return decorator

    def start(self, daemon: bool = False) -> None:
        """
        启动调度器

        :param daemon: 是否以守护模式运行
        """
        if self._running:
            logger.warning('调度器已经在运行中')
            return

        self._running = True
        self._shutdown_event.clear()

        logger.info('调度器启动')

        if daemon:
            thread = threading.Thread(
                target=self._run_loop,
                daemon=True,
                name='scheduler'
            )
            thread.start()
        else:
            self._run_loop()

    def stop(self) -> None:
        """停止调度器"""
        if not self._running:
            return

        logger.info('正在停止调度器...')
        self._running = False
        self._shutdown_event.set()
        logger.info('调度器已停止')

    def _run_loop(self) -> None:
        """主循环"""
        while self._running and not self._shutdown_event.is_set():
            try:
                self._check_and_run_jobs()
            except Exception as e:
                logger.error(f'调度器循环错误: {e}')

            _ = self._shutdown_event.wait(timeout=self.check_interval)

    def _check_and_run_jobs(self) -> None:
        """检查并执行到期的任务"""
        now = time.time()

        with self._lock:
            jobs_to_run = [
                job for job in self._jobs.values()
                if (
                    job.enabled
                    and job.next_run is not None
                    and job.next_run <= now
                    and job.running_count < job.max_instances
                )
            ]

        for job in jobs_to_run:
            self._run_job(job)

    def _run_job(self, job: ScheduledJob) -> None:
        """执行定时任务"""
        # 获取分布式锁
        lock_key = f'{self._lock_key}:{job.name}'
        lock_acquired: bool | None = self.redis.set(  # type: ignore[assignment]
            lock_key,
            '1',
            ex=60,
            nx=True
        )

        if not lock_acquired:
            # 其他实例正在执行
            return

        try:
            # 创建任务
            task = Task(
                name=job.task_name,
                payload=dict(job.payload),  # type: ignore[arg-type]
                priority=job.priority,
                metadata={
                    'scheduled_job': job.name,
                    'scheduled_time': time.time(),
                }
            )

            # 加入队列
            _ = self._queue.enqueue(task)

            # 更新任务状态
            with self._lock:
                job.last_run = time.time()
                job.running_count += 1

                # 计算下次执行时间
                if job.interval is not None:
                    job.next_run = time.time() + job.interval
                elif job.cron is not None:
                    next_dt = SimpleCronParser.get_next_run(job.cron)
                    job.next_run = next_dt.timestamp()

            next_run_time = job.next_run if job.next_run else time.time()
            logger.info(
                f'定时任务 {job.name} 已触发, ' +
                f'下次执行: {datetime.fromtimestamp(next_run_time)}'
            )

        finally:
            # 释放锁
            _ = self.redis.delete(lock_key)

    def get_jobs(self) -> list[dict[str, object]]:
        """获取所有定时任务信息"""
        with self._lock:
            return [
                {
                    'name': job.name,
                    'task_name': job.task_name,
                    'interval': job.interval,
                    'cron': job.cron,
                    'enabled': job.enabled,
                    'last_run': job.last_run,
                    'next_run': job.next_run,
                    'running_count': job.running_count,
                }
                for job in self._jobs.values()
            ]

    def run_job_now(self, job_name: str) -> bool:
        """
        立即执行指定任务

        :param job_name: 任务名称
        :return: 是否成功
        """
        with self._lock:
            if job_name not in self._jobs:
                return False
            job = self._jobs[job_name]

        self._run_job(job)
        return True


class RateLimiter:
    """
    速率限制器

    基于Redis实现的令牌桶算法。
    """

    KEY_PREFIX: str = 'rtq:rate_limiter'

    def __init__(
        self,
        name: str,
        rate: int,
        capacity: int,
        connection: RedisConnection | None = None
    ) -> None:
        """
        初始化速率限制器

        :param name: 限制器名称
        :param rate: 每秒填充的令牌数
        :param capacity: 桶容量
        :param connection: Redis连接实例
        """
        self.name: str = name
        self.rate: int = rate
        self.capacity: int = capacity
        self._connection: RedisConnection | None = connection

        self._tokens_key: str = f'{self.KEY_PREFIX}:{name}:tokens'
        self._last_update_key: str = f'{self.KEY_PREFIX}:{name}:last_update'

    @property
    def redis(self) -> Redis:  # type: ignore[type-arg]
        """获取Redis客户端"""
        if self._connection is not None:
            return self._connection.client
        return RedisConnection.get_client()

    def acquire(self, tokens: int = 1, block: bool = True) -> bool:
        """
        获取令牌

        :param tokens: 需要的令牌数
        :param block: 是否阻塞等待
        :return: 是否成功获取
        """
        while True:
            # 使用Lua脚本保证原子性
            script = """
            local tokens_key = KEYS[1]
            local last_update_key = KEYS[2]
            local rate = tonumber(ARGV[1])
            local capacity = tonumber(ARGV[2])
            local requested = tonumber(ARGV[3])
            local now = tonumber(ARGV[4])

            local current_tokens = tonumber(redis.call('get', tokens_key) or capacity)
            local last_update = tonumber(redis.call('get', last_update_key) or now)

            local elapsed = now - last_update
            local new_tokens = math.min(
                capacity,
                current_tokens + elapsed * rate
            )

            if new_tokens >= requested then
                redis.call('set', tokens_key, new_tokens - requested)
                redis.call('set', last_update_key, now)
                return 1
            else
                redis.call('set', tokens_key, new_tokens)
                redis.call('set', last_update_key, now)
                return 0
            end
            """

            result: int = self.redis.eval(  # type: ignore[assignment]
                script,
                2,
                self._tokens_key,
                self._last_update_key,
                self.rate,
                self.capacity,
                tokens,
                time.time()
            )

            if result == 1:
                return True

            if not block:
                return False

            # 等待令牌填充
            wait_time = tokens / self.rate
            time.sleep(min(wait_time, 1))

    def get_available_tokens(self) -> float:
        """获取当前可用令牌数"""
        current: str | None = self.redis.get(self._tokens_key)  # type: ignore[assignment]
        last_update: str | None = self.redis.get(self._last_update_key)  # type: ignore[assignment]

        if current is None:
            return float(self.capacity)

        current_val = float(current)
        last_update_val = float(last_update) if last_update else time.time()
        elapsed = time.time() - last_update_val

        return min(self.capacity, current_val + elapsed * self.rate)
