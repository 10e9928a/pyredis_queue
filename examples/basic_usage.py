# -*- coding: utf-8 -*-
"""
PyRedis Queue 使用示例

演示如何使用基于Redis的任务分发库。
"""

from __future__ import annotations

import time
import logging

from pyredis_queue import (
    RedisConnection,
    Task,
    TaskQueue,
    Worker,
    TaskScheduler,
    TaskPriority,
)
from pyredis_queue.task import task_handler
from pyredis_queue.worker import WorkerPool
from pyredis_queue.scheduler import RateLimiter

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# 1. 基础使用示例
# ============================================================================

def basic_example() -> None:
    """基础使用示例"""
    print('\n' + '=' * 60)
    print('1. 基础使用示例')
    print('=' * 60)

    # 初始化Redis连接
    conn = RedisConnection(host='9.135.106.116', port=6379, db=0)

    # 测试连接
    if not conn.ping():
        print('无法连接到Redis，请确保Redis服务已启动')
        return

    # 创建任务队列
    queue = TaskQueue(queue_name='demo')
    queue.clear_queue()  # 清空之前的数据

    # 注册任务处理器
    @task_handler('send_email')
    def send_email(to: str, subject: str, body: str) -> dict[str, str]:
        """发送邮件任务"""
        logger.info(f'发送邮件到 {to}: {subject}')
        # body参数用于实际发送邮件的内容
        _ = body  # 标记为已使用
        time.sleep(1)  # 模拟发送过程
        return {'status': 'sent', 'to': to}

    @task_handler('process_data')
    def process_data(data_id: int, operation: str) -> dict[str, object]:
        """处理数据任务"""
        logger.info(f'处理数据 {data_id}, 操作: {operation}')
        time.sleep(.5)
        return {'data_id': data_id, 'result': 'processed'}

    # 确保处理器被注册（避免未使用警告）
    _ = send_email
    _ = process_data

    # 创建并提交任务
    task1 = Task(
        name='send_email',
        payload={
            'to': 'user@example.com',
            'subject': '测试邮件',
            'body': '这是一封测试邮件'
        },
        priority=TaskPriority.HIGH
    )

    task2 = Task(
        name='process_data',
        payload={
            'data_id': 12345,
            'operation': 'transform'
        }
    )

    # 加入队列
    _ = queue.enqueue(task1)
    _ = queue.enqueue(task2)

    # 查看队列状态
    stats = queue.get_queue_stats()
    print(f'队列状态: {stats}')

    # 创建Worker并执行任务
    worker = Worker(queue_name='demo', concurrency=2)

    # 设置回调
    worker.on_task_success(
        lambda t, r: print(f'任务成功: {t.name} -> {r}')
    )
    worker.on_task_failure(
        lambda t, e: print(f'任务失败: {t.name} -> {e}')
    )

    # 启动Worker（守护模式）
    worker.start(daemon=True)

    # 等待任务完成
    time.sleep(5)

    # 停止Worker
    worker.stop()

    # 查看最终状态
    stats = queue.get_queue_stats()
    print(f'最终队列状态: {stats}')

    # 清理
    conn.close()


# ============================================================================
# 2. 延迟任务示例
# ============================================================================

def delayed_task_example() -> None:
    """延迟任务示例"""
    print('\n' + '=' * 60)
    print('2. 延迟任务示例')
    print('=' * 60)

    RedisConnection.reset()
    conn = RedisConnection(host='localhost', port=6379, db=0)

    if not conn.ping():
        print('无法连接到Redis')
        return

    queue = TaskQueue(queue_name='delayed_demo')
    queue.clear_queue()

    @task_handler('delayed_notification')
    def delayed_notification(message: str) -> dict[str, object]:
        """延迟通知任务"""
        logger.info(f'发送延迟通知: {message}')
        return {'message': message, 'sent_at': time.time()}

    # 确保处理器被注册
    _ = delayed_notification

    # 创建延迟任务
    task = Task(
        name='delayed_notification',
        payload={'message': '这是一条延迟5秒的通知'}
    )

    # 延迟5秒执行
    print('提交延迟任务，将在5秒后执行...')
    _ = queue.enqueue(task, delay=5)

    # 查看延迟队列
    stats = queue.get_queue_stats()
    print(f'队列状态: {stats}')

    # 创建Worker
    worker = Worker(queue_name='delayed_demo', concurrency=1)
    worker.start(daemon=True)

    # 等待任务执行
    time.sleep(8)

    worker.stop()
    conn.close()


# ============================================================================
# 3. 优先级队列示例
# ============================================================================

def priority_queue_example() -> None:
    """优先级队列示例"""
    print('\n' + '=' * 60)
    print('3. 优先级队列示例')
    print('=' * 60)

    RedisConnection.reset()
    conn = RedisConnection(host='localhost', port=6379, db=0)

    if not conn.ping():
        print('无法连接到Redis')
        return

    queue = TaskQueue(queue_name='priority_demo')
    queue.clear_queue()

    execution_order: list[str] = []

    @task_handler('priority_task')
    def priority_task(name: str, priority: int) -> dict[str, str]:
        """优先级任务"""
        execution_order.append(name)
        logger.info(f'执行任务: {name} (优先级: {priority})')
        return {'name': name}

    # 确保处理器被注册
    _ = priority_task

    # 创建不同优先级的任务
    tasks = [
        ('低优先级任务1', TaskPriority.LOW),
        ('普通任务1', TaskPriority.NORMAL),
        ('高优先级任务1', TaskPriority.HIGH),
        ('关键任务1', TaskPriority.CRITICAL),
        ('低优先级任务2', TaskPriority.LOW),
        ('高优先级任务2', TaskPriority.HIGH),
    ]

    for name, priority in tasks:
        task = Task(
            name='priority_task',
            payload={'name': name, 'priority': priority},
            priority=priority
        )
        _ = queue.enqueue(task)

    print(f'提交了 {len(tasks)} 个任务')

    # 使用单线程Worker确保顺序执行
    worker = Worker(queue_name='priority_demo', concurrency=1)
    worker.start(daemon=True)

    time.sleep(5)
    worker.stop()

    print(f'执行顺序: {execution_order}')

    conn.close()


# ============================================================================
# 4. 失败重试示例
# ============================================================================

def retry_example() -> None:
    """失败重试示例"""
    print('\n' + '=' * 60)
    print('4. 失败重试示例')
    print('=' * 60)

    RedisConnection.reset()
    conn = RedisConnection(host='localhost', port=6379, db=0)

    if not conn.ping():
        print('无法连接到Redis')
        return

    queue = TaskQueue(queue_name='retry_demo')
    queue.clear_queue()

    attempt_count = [0]

    @task_handler('unreliable_task')
    def unreliable_task(fail_times: int) -> dict[str, object]:
        """不稳定的任务，会失败指定次数"""
        attempt_count[0] += 1
        logger.info(f'尝试执行任务，第 {attempt_count[0]} 次')

        if attempt_count[0] <= fail_times:
            raise Exception(f'模拟失败 (第 {attempt_count[0]} 次)')

        return {'success': True, 'attempts': attempt_count[0]}

    # 确保处理器被注册
    _ = unreliable_task

    # 创建会失败2次的任务
    task = Task(
        name='unreliable_task',
        payload={'fail_times': 2},
        max_retries=3  # 最多重试3次
    )

    _ = queue.enqueue(task)

    worker = Worker(queue_name='retry_demo', concurrency=1)
    worker.start(daemon=True)

    time.sleep(10)
    worker.stop()

    # 查看结果
    result_task = queue.get_task(task.task_id)
    if result_task is not None:
        print(f'任务状态: {result_task.status}')
        print(f'重试次数: {result_task.retry_count}')
        print(f'执行结果: {result_task.result}')
    else:
        print('无法获取任务结果')

    conn.close()


# ============================================================================
# 5. 定时任务示例
# ============================================================================

def scheduled_task_example() -> None:
    """定时任务示例"""
    print('\n' + '=' * 60)
    print('5. 定时任务示例')
    print('=' * 60)

    RedisConnection.reset()
    conn = RedisConnection(host='localhost', port=6379, db=0)

    if not conn.ping():
        print('无法连接到Redis')
        return

    queue = TaskQueue(queue_name='scheduled_demo')
    queue.clear_queue()

    from pyredis_queue.scheduler import ScheduledJob

    @task_handler('heartbeat')
    def heartbeat() -> dict[str, float]:
        """心跳任务"""
        logger.info(f'心跳检查 - {time.strftime("%H:%M:%S")}')
        return {'timestamp': time.time()}

    # 确保处理器被注册
    _ = heartbeat

    # 创建调度器
    scheduler = TaskScheduler(queue_name='scheduled_demo')

    # 添加周期性任务（每2秒执行一次）
    job = ScheduledJob(
        name='heartbeat_job',
        task_name='heartbeat',
        interval=2,
        run_immediately=True
    )
    scheduler.add_job(job)

    # 启动调度器和Worker
    scheduler.start(daemon=True)

    worker = Worker(queue_name='scheduled_demo', concurrency=1)
    worker.start(daemon=True)

    # 运行10秒
    print('定时任务运行中，将持续10秒...')
    time.sleep(10)

    # 停止
    scheduler.stop()
    worker.stop()

    print('定时任务已停止')
    conn.close()


# ============================================================================
# 6. WorkerPool示例
# ============================================================================

def worker_pool_example() -> None:
    """Worker池示例"""
    print('\n' + '=' * 60)
    print('6. Worker池示例')
    print('=' * 60)

    RedisConnection.reset()
    conn = RedisConnection(host='localhost', port=6379, db=0)

    if not conn.ping():
        print('无法连接到Redis')
        return

    # 清空队列
    for qname in ['queue_a', 'queue_b']:
        q = TaskQueue(queue_name=qname)
        q.clear_queue()

    @task_handler('task_a')
    def task_a(value: int) -> dict[str, object]:
        """队列A任务处理器"""
        logger.info(f'队列A任务: {value}')
        time.sleep(.5)
        return {'queue': 'A', 'value': value}

    @task_handler('task_b')
    def task_b(value: int) -> dict[str, object]:
        """队列B任务处理器"""
        logger.info(f'队列B任务: {value}')
        time.sleep(.5)
        return {'queue': 'B', 'value': value}

    # 确保处理器被注册
    _ = task_a
    _ = task_b

    # 向不同队列提交任务
    queue_a = TaskQueue(queue_name='queue_a')
    queue_b = TaskQueue(queue_name='queue_b')

    for i in range(5):
        _ = queue_a.enqueue(Task(name='task_a', payload={'value': i}))
        _ = queue_b.enqueue(Task(name='task_b', payload={'value': i * 10}))

    # 创建Worker池
    pool = WorkerPool(
        queue_names=['queue_a', 'queue_b'],
        workers_per_queue=1,
        concurrency=2
    )

    pool.start()

    # 运行一段时间
    time.sleep(10)

    # 查看统计
    stats = pool.get_stats()
    print(f'Worker池统计: {stats}')

    pool.stop()
    conn.close()


# ============================================================================
# 7. 速率限制示例
# ============================================================================

def rate_limiter_example() -> None:
    """速率限制示例"""
    print('\n' + '=' * 60)
    print('7. 速率限制示例')
    print('=' * 60)

    RedisConnection.reset()
    conn = RedisConnection(host='localhost', port=6379, db=0)

    if not conn.ping():
        print('无法连接到Redis')
        return

    # 创建速率限制器：每秒2个请求，最多积累5个令牌
    limiter = RateLimiter(
        name='api_limiter',
        rate=2,
        capacity=5
    )

    print('速率限制器: 每秒2个请求，容量5')
    print('尝试发送10个请求...')

    for i in range(10):
        start = time.time()
        if limiter.acquire(tokens=1, block=True):
            elapsed = time.time() - start
            print(f'请求 {i + 1}: 成功 (等待 {elapsed:.2f}秒)')
        else:
            print(f'请求 {i + 1}: 被限制')

    conn.close()


# ============================================================================
# 主程序
# ============================================================================

def main() -> None:
    """运行所有示例"""
    print('PyRedis Queue 示例程序')
    print('请确保Redis服务已启动在 localhost:6379')

    try:
        # 运行各个示例
        basic_example()
        delayed_task_example()
        priority_queue_example()
        retry_example()
        scheduled_task_example()
        worker_pool_example()
        rate_limiter_example()

    except Exception as e:
        logger.error(f'示例运行出错: {e}')
        raise

    print('\n' + '=' * 60)
    print('所有示例运行完成！')
    print('=' * 60)


if __name__ == '__main__':
    main()
