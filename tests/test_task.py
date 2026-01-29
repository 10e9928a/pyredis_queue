# -*- coding: utf-8 -*-
"""
单元测试 - Task模块
"""

from __future__ import annotations

import time

from pyredis_queue.task import (
    Task,
    TaskStatus,
    TaskPriority,
    TaskRegistry,
    task_handler,
)


class TestTask:
    """Task类测试"""

    def test_task_creation(self) -> None:
        """测试任务创建"""
        task = Task(
            name='test_task',
            payload={'key': 'value'}
        )

        assert task.name == 'test_task'
        assert task.payload == {'key': 'value'}
        assert task.status == TaskStatus.PENDING
        assert task.priority == TaskPriority.NORMAL
        assert task.task_id is not None
        assert task.created_at > 0

    def test_task_priority(self) -> None:
        """测试任务优先级"""
        low_task = Task(name='low', priority=TaskPriority.LOW)
        high_task = Task(name='high', priority=TaskPriority.HIGH)

        assert low_task.priority < high_task.priority

    def test_task_serialization(self) -> None:
        """测试任务序列化"""
        task = Task(
            name='serialize_test',
            payload={'data': 123},
            priority=TaskPriority.HIGH
        )

        # 序列化
        json_str = task.to_json()
        assert isinstance(json_str, str)

        # 反序列化
        restored = Task.from_json(json_str)
        assert restored.name == task.name
        assert restored.payload == task.payload
        assert restored.priority == task.priority
        assert restored.task_id == task.task_id

    def test_task_dict_conversion(self) -> None:
        """测试字典转换"""
        task = Task(name='dict_test', payload={'x': 1})

        # 转为字典
        data = task.to_dict()
        assert isinstance(data, dict)
        assert data['name'] == 'dict_test'

        # 从字典创建
        restored = Task.from_dict(data)
        assert restored.name == task.name

    def test_task_status_transitions(self) -> None:
        """测试任务状态转换"""
        task = Task(name='status_test')

        # 初始状态
        assert task.status == TaskStatus.PENDING

        # 运行中
        task.mark_running()
        assert task.status == TaskStatus.RUNNING
        assert task.started_at is not None

        # 成功
        task.mark_success(result='done')
        assert task.status == TaskStatus.SUCCESS
        assert task.result == 'done'
        assert task.finished_at is not None

    def test_task_failure(self) -> None:
        """测试任务失败"""
        task = Task(name='fail_test')
        task.mark_running()
        task.mark_failed('something went wrong')

        assert task.status == TaskStatus.FAILED
        assert task.error == 'something went wrong'

    def test_task_retry(self) -> None:
        """测试任务重试"""
        task = Task(name='retry_test', max_retries=3)

        assert task.can_retry() is True
        assert task.retry_count == 0

        # 重试
        task.mark_retrying()
        assert task.retry_count == 1
        assert task.status == TaskStatus.RETRYING
        assert task.can_retry() is True

        # 多次重试
        task.mark_retrying()
        task.mark_retrying()
        assert task.retry_count == 3
        assert task.can_retry() is False

    def test_task_duration(self) -> None:
        """测试任务执行时长"""
        task = Task(name='duration_test')

        # 未开始
        assert task.duration is None

        # 开始
        task.mark_running()
        time.sleep(.1)

        # 运行中
        assert task.duration is not None
        assert task.duration >= .1

        # 完成
        task.mark_success()
        final_duration = task.duration
        time.sleep(.1)
        assert task.duration == final_duration


class TestTaskRegistry:
    """TaskRegistry类测试"""

    def setup_method(self) -> None:
        """每个测试前清空注册表"""
        TaskRegistry.clear()

    def test_register_handler(self) -> None:
        """测试注册处理器"""
        @task_handler('my_task')
        def my_handler(x: int) -> int:
            return x * 2

        # 确保处理器被注册（避免未使用警告）
        _ = my_handler

        handler = TaskRegistry.get_handler('my_task')
        assert handler is not None
        assert handler(5) == 10

    def test_list_handlers(self) -> None:
        """测试列出处理器"""
        @task_handler('task1')
        def handler1() -> None:
            pass

        @task_handler('task2')
        def handler2() -> None:
            pass

        # 确保处理器被注册（避免未使用警告）
        _ = handler1
        _ = handler2

        handlers = TaskRegistry.list_handlers()
        assert 'task1' in handlers
        assert 'task2' in handlers

    def test_get_nonexistent_handler(self) -> None:
        """测试获取不存在的处理器"""
        handler = TaskRegistry.get_handler('nonexistent')
        assert handler is None
