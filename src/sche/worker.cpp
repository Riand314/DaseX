#include "worker.hpp"
#include "numa_help.hpp"
#include "scheduler.hpp"

namespace DaseX {

void bind_memory_to_node(bitmask* mask, int node_for_memory) {
    numa_bitmask_setbit(mask, node_for_memory);
    numa_set_membind(mask);
}

// 一个Worker绑定一个Core，一个Core有且只有一个Worker，Worker负责执行实际的任务
Worker::Worker(int worker_id_, Scheduler *scheduler_)
        : worker_id(worker_id_), scheduler(scheduler_), is_running(true) {
    // 启动工作线程
    work_state = WorkerState::READY;
    queue_hard = std::make_shared<SafeQueue<std::function<void()>>>();
    queue_soft = std::make_shared<SafeQueue<std::function<void()>>>();
    worker_thread = std::thread(&Worker::work, this);
}

void Worker::work() {
    Util::bind_thread_to_cpu(worker_id);
//    if(worker_id >= 31) {
//        bitmask* mask = numa_allocate_nodemask();
//        bind_memory_to_node(mask, 2);
//    } else {
//        Util::bind_memory_to_cpu();
//    }
    Util::bind_memory_to_cpu();
    // spdlog::info("The work {} is running on CPU {}.", worker_id,
    // sched_getcpu());
    while (is_running) {
        std::function<void()> task;
        bool dequeued;
        {
            std::unique_lock<std::mutex> lock(worker_mutex);
            if (queue_hard->empty() && queue_soft->empty()) {
                // spdlog::info("wait task ========= {}.", worker_id);
                work_state = WorkerState::IDLE;
                cv.wait(lock);
            }
            // 取出任务队列中的元素
            dequeued = queue_hard->dequeue(task);
            if (!dequeued) {
                dequeued = queue_soft->dequeue(task);
            }
        }
        if (dequeued && task) {
            // spdlog::info("do task ========= {}.", worker_id);
            work_state = WorkerState::RUNNING;
            task(); // 执行任务
            cv.notify_all();
        }
    } // while
    work_state = WorkerState::DEAD;
}

} // namespace DaseX
