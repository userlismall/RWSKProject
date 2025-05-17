# -*- coding: utf-8 -*-

import threading
import queue
import time
import concurrent.futures
import logging


class GeneralRequestProcessor:
    def __init__(self, max_concurrent_requests=10):
        """
        通用请求处理器
        :param max_concurrent_requests: 最大并发请求数
        """
        self.max_concurrent_requests = max_concurrent_requests
        self.request_queue = queue.Queue()
        self.lock = threading.Lock()
        self.total_requests = 0
        self.completed_requests = 0
        self.active_requests = 0
        self.start_time = time.time()
        self.results = []  # 用于保存每个请求的输入和输出

        # 配置日志
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
        self.logger = logging.getLogger()

    def apply(self, handle_function, data_list):
        """
        应用请求处理器
        :param handle_function: 处理每个请求的函数
        :param data_list: 请求的数据列表
        """
        self.handle_function = handle_function
        self.data_list = data_list
        self.total_requests = len(data_list)

        # 将所有请求数据登记到队列中
        for request_data in self.data_list:
            self.request_queue.put(request_data)

    def process_request(self, request_data):
        """处理单个请求，调用用户传入的handle_function"""
        self.active_requests += 1
        try:
            response = self.handle_function(*request_data[:-1])
        except Exception as e:
            response = ""
            print(e)
        
        self.completed_requests += 1
        self.active_requests -= 1

        # 保存输入和响应作为字典
        self.results.append({"input": request_data, "output": response})
        
        self.log_progress()

    def log_progress(self):
        """打印处理进度，包括已完成请求、正在处理请求、剩余请求和预计时间"""
        remaining_requests = self.total_requests - self.completed_requests
        elapsed_time = time.time() - self.start_time
        avg_time_per_request = elapsed_time / self.completed_requests if self.completed_requests > 0 else 0
        estimated_time_remaining = remaining_requests * avg_time_per_request if avg_time_per_request > 0 else 0

        # 打印日志，增加“正在处理”的数量
        self.logger.info(
            f"Processed {self.completed_requests}/{self.total_requests} requests. "
            f"{self.active_requests} currently processing. {remaining_requests} remaining. "
            f"Estimated time left: {estimated_time_remaining:.2f} seconds."
        )

    def process_all_requests(self):
        """启动线程池处理所有请求"""
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrent_requests) as executor:
            while True:
                if not self.request_queue.empty():
                    request_data = self.request_queue.get()
                    executor.submit(self.process_request, request_data)
                elif self.completed_requests >= self.total_requests:
                    break  # 如果所有请求都处理完毕，退出循环

    def get_results(self):
        """获取所有请求的输入和响应的字典列表"""
        return self.results