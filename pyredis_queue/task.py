# -*- coding: utf-8 -*-
"""
任务定义模块

提供任务的数据结构和状态定义。
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum, IntEnum
from typing import Callable, TypeVar, cast, override

# 泛型类型变量，用于装饰器
F = TypeVar('F', bound=Callable[..., object])


class TaskStatus(str, Enum):
    """任务状态枚举"""
    PENDING = 'pending'      # 等待执行
    RUNNING = 'running'      # 正在执行
    SUCCESS = 'success'      # 执行成功
    FAILED = 'failed'        # 执行失败
    RETRYING = 'retrying'    # 重试中
    CANCELLED = 'cancelled'  # 已取消
    TIMEOUT = 'timeout'      # 超时


class TaskPriority(IntEnum):
    """任务优先级枚举"""
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 100


@dataclass
class Task:
    """
    任务数据类

    :param name: 任务名称
    :param payload: 任务数据
    :param task_id: 任务ID（自动生成）
    :param priority: 任务优先级
    :param max_retries: 最大重试次数
    :param retry_count: 当前重试次数
    :param timeout: 任务超时时间（秒）
    :param status: 任务状态
    :param result: 任务执行结果
    :param error: 错误信息
    :param created_at: 创建时间
    :param started_at: 开始时间
    :param finished_at: 完成时间
    :param metadata: 额外元数据
    """
    name: str
    payload: dict[str, object] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    priority: int = TaskPriority.NORMAL
    max_retries: int = 3
    retry_count: int = 0
    timeout: int = 300
    status: str = TaskStatus.PENDING
    result: object = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    def to_json(self) -> str:
        """序列化为JSON字符串"""
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> Task:
        """从JSON字符串反序列化"""
        data = cast(dict[str, object], json.loads(json_str))
        return cls(
            name=cast(str, data.get('name', '')),
            payload=cast(dict[str, object], data.get('payload', {})),
            task_id=cast(str, data.get('task_id', str(uuid.uuid4()))),
            priority=cast(int, data.get('priority', TaskPriority.NORMAL)),
            max_retries=cast(int, data.get('max_retries', 3)),
            retry_count=cast(int, data.get('retry_count', 0)),
            timeout=cast(int, data.get('timeout', 300)),
            status=cast(str, data.get('status', TaskStatus.PENDING)),
            result=data.get('result'),
            error=cast(str | None, data.get('error')),
            created_at=cast(float, data.get('created_at', time.time())),
            started_at=cast(float | None, data.get('started_at')),
            finished_at=cast(float | None, data.get('finished_at')),
            metadata=cast(dict[str, object], data.get('metadata', {})),
        )

    def to_dict(self) -> dict[str, object]:
        """转换为字典"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> Task:
        """从字典创建任务"""
        return cls(
            name=cast(str, data.get('name', '')),
            payload=cast(dict[str, object], data.get('payload', {})),
            task_id=cast(str, data.get('task_id', str(uuid.uuid4()))),
            priority=cast(int, data.get('priority', TaskPriority.NORMAL)),
            max_retries=cast(int, data.get('max_retries', 3)),
            retry_count=cast(int, data.get('retry_count', 0)),
            timeout=cast(int, data.get('timeout', 300)),
            status=cast(str, data.get('status', TaskStatus.PENDING)),
            result=data.get('result'),
            error=cast(str | None, data.get('error')),
            created_at=cast(float, data.get('created_at', time.time())),
            started_at=cast(float | None, data.get('started_at')),
            finished_at=cast(float | None, data.get('finished_at')),
            metadata=cast(dict[str, object], data.get('metadata', {})),
        )

    def can_retry(self) -> bool:
        """检查是否可以重试"""
        return self.retry_count < self.max_retries

    def mark_running(self) -> None:
        """标记为运行中"""
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()

    def mark_success(self, result: object = None) -> None:
        """标记为成功"""
        self.status = TaskStatus.SUCCESS
        self.result = result
        self.finished_at = time.time()

    def mark_failed(self, error: str) -> None:
        """标记为失败"""
        self.status = TaskStatus.FAILED
        self.error = error
        self.finished_at = time.time()

    def mark_retrying(self) -> None:
        """标记为重试中"""
        self.status = TaskStatus.RETRYING
        self.retry_count += 1

    def mark_cancelled(self) -> None:
        """标记为已取消"""
        self.status = TaskStatus.CANCELLED
        self.finished_at = time.time()

    def mark_timeout(self) -> None:
        """标记为超时"""
        self.status = TaskStatus.TIMEOUT
        self.finished_at = time.time()

    @property
    def duration(self) -> float | None:
        """获取任务执行时长"""
        if self.started_at is None:
            return None
        end_time = self.finished_at or time.time()
        return end_time - self.started_at

    @override
    def __repr__(self) -> str:
        return (
            f"Task(id={self.task_id[:8]}..., name={self.name}, "
            f"status={self.status}, priority={self.priority})"
        )


class TaskRegistry:
    """任务处理器注册表"""

    _handlers: dict[str, Callable[..., object]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[F], F]:
        """
        注册任务处理器的装饰器

        :param name: 任务名称
        :return: 装饰器函数
        """
        def decorator(func: F) -> F:
            cls._handlers[name] = func
            return func
        return decorator

    @classmethod
    def get_handler(cls, name: str) -> Callable[..., object] | None:
        """获取任务处理器"""
        return cls._handlers.get(name)

    @classmethod
    def list_handlers(cls) -> dict[str, Callable[..., object]]:
        """列出所有处理器"""
        return cls._handlers.copy()

    @classmethod
    def clear(cls) -> None:
        """清空注册表"""
        cls._handlers.clear()


def task_handler(name: str) -> Callable[[F], F]:
    """装饰器：注册任务处理器"""
    return TaskRegistry.register(name)
