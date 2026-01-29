# -*- coding: utf-8 -*-
"""
任务队列模块

提供基于Redis的任务队列实现。
"""

from __future__ import annotations

import time
import logging
from redis import Redis
from pyredis_queue.connection import RedisConnection
from pyredis_queue.task import Task, TaskStatus



logger = logging.getLogger(__name__)


class TaskQueue:
    """
    基于Redis的任务队列

    支持优先级队列、延迟队列、死信队列等功能。
    """

    # Redis键前缀
    KEY_PREFIX: str = 'rtq'

    def __init__(
        self,
        queue_name: str = 'default',
        connection: RedisConnection | None = None
    ) -> None:
        """
        初始化任务队列

        :param queue_name: 队列名称
        :param connection: Redis连接实例
        """
        self.queue_name: str = queue_name
        self._connection: RedisConnection | None = connection

        # 队列键名
        self._pending_key: str = f'{self.KEY_PREFIX}:{queue_name}:pending'
        self._running_key: str = f'{self.KEY_PREFIX}:{queue_name}:running'
        self._success_key: str = f'{self.KEY_PREFIX}:{queue_name}:success'
        self._failed_key: str = f'{self.KEY_PREFIX}:{queue_name}:failed'
        self._delayed_key: str = f'{self.KEY_PREFIX}:{queue_name}:delayed'
        self._dead_letter_key: str = f'{self.KEY_PREFIX}:{queue_name}:dead_letter'
        self._task_data_key: str = f'{self.KEY_PREFIX}:{queue_name}:task_data'

    @property
    def redis(self) -> Redis:  # type: ignore[type-arg]
        """获取Redis客户端"""
        if self._connection is not None:
            return self._connection.client
        return RedisConnection.get_client()

    def enqueue(
        self,
        task: Task,
        delay: int = 0
    ) -> str:
        """
        将任务加入队列

        :param task: 任务对象
        :param delay: 延迟执行时间（秒）
        :return: 任务ID
        """
        # 保存任务数据
        _ = self.redis.hset(
            self._task_data_key,
            task.task_id,
            task.to_json()
        )

        if delay > 0:
            # 延迟任务，使用有序集合
            execute_at = time.time() + delay
            _ = self.redis.zadd(
                self._delayed_key,
                {task.task_id: execute_at}
            )
            logger.debug(f'任务 {task.task_id} 将在 {delay} 秒后执行')
        else:
            # 立即执行，按优先级排序
            # 使用负数优先级，使高优先级任务排在前面
            score = -task.priority + (time.time() / 1e10)
            _ = self.redis.zadd(
                self._pending_key,
                {task.task_id: score}
            )
            logger.debug(f'任务 {task.task_id} 已加入队列')

        return task.task_id

    def dequeue(self, timeout: int = 0) -> Task | None:
        """
        从队列取出任务

        :param timeout: 阻塞超时时间（秒），0表示非阻塞
        :return: 任务对象或None
        """
        # 首先处理延迟队列中到期的任务
        _ = self._process_delayed_tasks()

        # 从待处理队列取出优先级最高的任务
        result: list[tuple[str, float]] = self.redis.zpopmin(  # type: ignore[assignment]
            self._pending_key, count=1
        )

        task_id: str
        if not result:
            if timeout > 0:
                # 阻塞等待
                block_result: tuple[str, str, float] | None = self.redis.bzpopmin(  # type: ignore[assignment]
                    self._pending_key,
                    timeout=timeout
                )
                if block_result:
                    # bzpopmin 返回格式为 (key, member, score)
                    task_id = str(block_result[1])
                else:
                    return None
            else:
                return None
        else:
            # zpopmin 返回格式为 [(member, score)]
            task_id = str(result[0][0])

        # 获取任务数据
        task_data: str | None = self.redis.hget(self._task_data_key, task_id)  # type: ignore[assignment]
        if task_data is None:
            logger.warning(f'任务数据不存在: {task_id}')
            return None

        task = Task.from_json(str(task_data))

        # 将任务移动到运行队列
        _ = self.redis.zadd(
            self._running_key,
            {task_id: time.time()}
        )

        task.mark_running()
        self._update_task(task)

        logger.debug(f'任务 {task_id} 开始执行')
        return task

    def complete(self, task: Task, result: object = None) -> None:
        """
        标记任务完成

        :param task: 任务对象
        :param result: 执行结果
        """
        task.mark_success(result)
        self._update_task(task)

        # 从运行队列移除
        _ = self.redis.zrem(self._running_key, task.task_id)

        # 添加到成功队列
        _ = self.redis.zadd(
            self._success_key,
            {task.task_id: time.time()}
        )

        logger.debug(f'任务 {task.task_id} 执行成功')

    def fail(self, task: Task, error: str) -> None:
        """
        标记任务失败

        :param task: 任务对象
        :param error: 错误信息
        """
        # 从运行队列移除
        _ = self.redis.zrem(self._running_key, task.task_id)

        if task.can_retry():
            # 可以重试
            task.mark_retrying()
            self._update_task(task)

            # 重新加入待处理队列，降低优先级
            score = -task.priority + task.retry_count + (time.time() / 1e10)
            _ = self.redis.zadd(
                self._pending_key,
                {task.task_id: score}
            )
            logger.info(
                f'任务 {task.task_id} 将进行第 {task.retry_count} 次重试'
            )
        else:
            # 不能重试，标记失败
            task.mark_failed(error)
            self._update_task(task)

            # 添加到失败队列
            _ = self.redis.zadd(
                self._failed_key,
                {task.task_id: time.time()}
            )

            # 同时添加到死信队列
            _ = self.redis.zadd(
                self._dead_letter_key,
                {task.task_id: time.time()}
            )

            logger.warning(f'任务 {task.task_id} 执行失败: {error}')

    def cancel(self, task_id: str) -> bool:
        """
        取消任务

        :param task_id: 任务ID
        :return: 是否成功取消
        """
        # 从待处理队列移除
        removed: int = self.redis.zrem(self._pending_key, task_id)  # type: ignore[assignment]

        if removed:
            task = self.get_task(task_id)
            if task:
                task.mark_cancelled()
                self._update_task(task)
            logger.info(f'任务 {task_id} 已取消')
            return True

        # 从延迟队列移除
        removed = self.redis.zrem(self._delayed_key, task_id)  # type: ignore[assignment]
        if removed:
            task = self.get_task(task_id)
            if task:
                task.mark_cancelled()
                self._update_task(task)
            logger.info(f'延迟任务 {task_id} 已取消')
            return True

        return False

    def get_task(self, task_id: str) -> Task | None:
        """
        获取任务信息

        :param task_id: 任务ID
        :return: 任务对象或None
        """
        task_data: str | None = self.redis.hget(self._task_data_key, task_id)  # type: ignore[assignment]
        if task_data:
            return Task.from_json(str(task_data))
        return None

    def _update_task(self, task: Task) -> None:
        """更新任务数据"""
        _ = self.redis.hset(
            self._task_data_key,
            task.task_id,
            task.to_json()
        )

    def _process_delayed_tasks(self) -> int:
        """
        处理到期的延迟任务

        :return: 处理的任务数量
        """
        now = time.time()

        # 获取所有到期的延迟任务
        task_ids: list[str] = self.redis.zrangebyscore(  # type: ignore[assignment]
            self._delayed_key,
            '-inf',
            now
        )

        count = 0
        for task_id in task_ids:
            # 从延迟队列移除
            if self.redis.zrem(self._delayed_key, str(task_id)):
                # 获取任务信息
                task = self.get_task(str(task_id))
                if task:
                    # 加入待处理队列
                    score = -task.priority + (time.time() / 1e10)
                    _ = self.redis.zadd(
                        self._pending_key,
                        {str(task_id): score}
                    )
                    count += 1
                    logger.debug(f'延迟任务 {task_id} 已加入待处理队列')

        return count

    def get_queue_stats(self) -> dict[str, int]:
        """
        获取队列统计信息

        :return: 统计信息字典
        """
        return {
            'pending': int(self.redis.zcard(self._pending_key)),  # type: ignore[arg-type]
            'running': int(self.redis.zcard(self._running_key)),  # type: ignore[arg-type]
            'success': int(self.redis.zcard(self._success_key)),  # type: ignore[arg-type]
            'failed': int(self.redis.zcard(self._failed_key)),  # type: ignore[arg-type]
            'delayed': int(self.redis.zcard(self._delayed_key)),  # type: ignore[arg-type]
            'dead_letter': int(self.redis.zcard(self._dead_letter_key)),  # type: ignore[arg-type]
        }

    def get_pending_tasks(
        self,
        start: int = 0,
        end: int = -1
    ) -> list[Task]:
        """
        获取待处理任务列表

        :param start: 起始索引
        :param end: 结束索引
        :return: 任务列表
        """
        task_ids: list[str] = self.redis.zrange(  # type: ignore[assignment]
            self._pending_key,
            start,
            end
        )
        tasks: list[Task] = []
        for tid in task_ids:
            task = self.get_task(str(tid))
            if task is not None:
                tasks.append(task)
        return tasks

    def get_failed_tasks(
        self,
        start: int = 0,
        end: int = -1
    ) -> list[Task]:
        """
        获取失败任务列表

        :param start: 起始索引
        :param end: 结束索引
        :return: 任务列表
        """
        task_ids: list[str] = self.redis.zrange(  # type: ignore[assignment]
            self._failed_key,
            start,
            end
        )
        tasks: list[Task] = []
        for tid in task_ids:
            task = self.get_task(str(tid))
            if task is not None:
                tasks.append(task)
        return tasks

    def retry_failed_task(self, task_id: str) -> bool:
        """
        重试失败的任务

        :param task_id: 任务ID
        :return: 是否成功
        """
        task = self.get_task(task_id)
        if task is None:
            return False

        # 从失败队列移除
        _ = self.redis.zrem(self._failed_key, task_id)
        _ = self.redis.zrem(self._dead_letter_key, task_id)

        # 重置任务状态
        task.status = TaskStatus.PENDING
        task.retry_count = 0
        task.error = None
        task.result = None
        task.started_at = None
        task.finished_at = None

        self._update_task(task)

        # 重新加入队列
        _ = self.enqueue(task)

        logger.info(f'任务 {task_id} 已重新加入队列')
        return True

    def clear_queue(self) -> None:
        """清空队列"""
        keys = [
            self._pending_key,
            self._running_key,
            self._success_key,
            self._failed_key,
            self._delayed_key,
            self._dead_letter_key,
            self._task_data_key,
        ]
        for key in keys:
            _ = self.redis.delete(key)
        logger.info(f'队列 {self.queue_name} 已清空')

    def cleanup_completed(self, max_age: int = 86400) -> int:
        """
        清理已完成的任务

        :param max_age: 最大保留时间（秒）
        :return: 清理的任务数量
        """
        cutoff = time.time() - max_age
        count = 0

        # 清理成功队列
        old_tasks: list[str] = self.redis.zrangebyscore(  # type: ignore[assignment]
            self._success_key,
            '-inf',
            cutoff
        )
        for task_id in old_tasks:
            _ = self.redis.zrem(self._success_key, str(task_id))
            _ = self.redis.hdel(self._task_data_key, str(task_id))
            count += 1

        logger.info(f'清理了 {count} 个已完成的任务')
        return count
