// Created by root on 24-3-21.
//
/*
 * @Author: caiwanli 651943559@qq.com
 * @Date: 2024-03-18 21:21:20
 * @LastEditors: caiwanli 651943559@qq.com
 * @LastEditTime: 2024-03-26 10:06:47
 * @FilePath: /task_sche/src/pipeline/execute/pipeline_group_execute.cpp
 * @Description:
 */
#include "pipeline_group_execute.hpp"
#include "execute_context.hpp"
#include "physical_table_scan.hpp"
#include "join/radixjoin/physical_radix_join.hpp"
#include "pipeline.hpp"
#include "pipeline_task.hpp"
#include "merge_sort_task.hpp"
#include <thread>
#include <chrono>

namespace DaseX {

void PipelineGroupExecute::plan_dfs(std::shared_ptr<PipelineGroup> root, int group_id) {
    root->executor = this->getptr();
    root->group_id = group_id;
    pipe_stack.push(root);
    group_id++;
    // 递归访问子节点
    for (auto &weak_dependency : root->dependencies) {
        plan_dfs(weak_dependency, group_id);
    }
}

void PipelineGroupExecute::traverse_plan() {
    // 清空栈
    while (!pipe_stack.empty()) {
        pipe_stack.pop();
    }
    // 使用 DFS 遍历整个计划并将结果保存在 pipe_stack 中
    plan_dfs(plan, 0);
}

void PipelineGroupExecute::execute() {
    while (!pipe_stack.empty()) {
        std::shared_ptr<PipelineGroup> node = pipe_stack.top();
        pipe_stack.pop();
        // 执行之前需要初始化PipelineGroup
//        auto start = std::chrono::high_resolution_clock::now();
        node->Initialize();
//        auto  end = std::chrono::high_resolution_clock::now();
//        std::chrono::duration<double> duration = end - start;
//        spdlog::info("pipeline {}  Initialize time: {}", node->group_id, duration.count());
        int size = node->pipelines.size();
        std::vector<PipelineTask> pipeline_tasks;
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < size; i++) {
            int work_id = node->pipelines[i]->work_id;
            //      std::shared_ptr<ExecuteContext> execute_context_temp = std::make_shared<ExecuteContext>();
            //      execute_context_temp->set_pipeline(node->pipelines[i]);
            PipelineTask pipeline_task(node->pipelines[i]);
            pipeline_tasks.push_back(pipeline_task);
            // spdlog::info("[{} : {}] ==============pipeline work_id: {}", __FILE__, __LINE__, work_id);
            scheduler->submit_task(pipeline_task, work_id, true);
            ////// 这里出现段错误，多线程循环提交任务的话，一定要注意生命周期，循环内封装成匿名函数提交任务，
            ////// 但匿名函数一过循环就销毁，因此线程在执行时就会出现段错误。修改：在循环外定义一个数组用来保存
            ////// 需要提交的任务，这样即使循环结束任务销毁，但内存中的内容依然保留。
            // auto task_function = [&pipeline_task]()
            // { pipeline_task(); };
            // spdlog::info("[{} : {}] ==============pipeline work_id: {}", __FILE__, __LINE__, pipeline->work_id); scheduler->submit_task(task_function, pipeline->work_id, true);
            // std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        // TODO:加锁同步，完成一个pipeline_group后，再执行下一个pipeline_group
        {
            std::unique_lock<std::mutex> lock(lock_);
            cv_.wait(lock, [this, &node] { return node->is_finish(); });
        }
        auto end = std::chrono::high_resolution_clock::now();
        // 计算时间差（即运行时间）
        duration += (end - start);
        // 查看注册事件，例如MergeSort，MergeJoin等
        if(node->event != TaskType::Invalid) {
            switch (node->event) {
                case TaskType::MergeSortTask :
                {
                    // TODO: 提交合并排序任务给线程池，并且需要等待排序任务完成
                    MergeSortTask merge_sort_task(node);
                    scheduler->submit_task(merge_sort_task, node->pipelines[0]->work_id, true);
                    // 等待合并排序任务完成
                    {
                        std::unique_lock<std::mutex> lock_sort(lock_);
                        cv_.wait(lock_sort, [this, &node] { return node->extra_task; });
                    }
                    break;
                }
                case TaskType::MergeJoinTask :
                {
                    // TODO: 后续可能会有MergeJoinTask
                    break;
                }
                default:
                    break;
            }
        } // if(node->event != TaskType::Invalid)
        // spdlog::info("[{} : {}] ==============pipelinegroup is finish: {}", __FILE__, __LINE__, node->group_id);
    }
    // spdlog::info("execute finsh!what a great work!!\n");
}


} // DaseX
