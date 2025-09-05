/*
* @Author: caiwanli 651943559@qq.com
* @Date: 2024-03-18 15:03:22
* @LastEditors: caiwanli 651943559@qq.com
* @LastEditTime: 2024-03-18 16:34:51
* @FilePath: /task_sche/src/pipeline/pipeline_group.cpp
* @Description:
*/
#include "pipeline_group.hpp"
#include "pipeline.hpp"
#include "physical_table_scan.hpp"
#include "physical_order.hpp"

namespace DaseX {

PipelineGroup::PipelineGroup() : parallelism(0), comlete_pipe(0) {}

void PipelineGroup::Initialize() {
    // 根据source的类型来判断初始化
    // 如果是TableScan，根据表分区来确定并行度，如果不是，只需要从parent中获取相关信息
    //     Step1: 根据并行度Copy出来对应的pipeline数据量
    //     Step2: 依次初始化每条pipeline
    if(dependencies.empty()) {
        is_root = true;
    }
    int parallelism_count = 0;
    if(pipelines[0]->source->type == PhysicalOperatorType::TABLE_SCAN) {
        // 获取表分区
        std::shared_ptr<Table> &table = (*(pipelines[0]->source)).Cast<PhysicalTableScan>().table;
        std::vector<int> partition_info;
        int partition_num = table->partition_num;
        std::vector<int> &partition_bitmap = table->partition_bitmap;
        for(int i = 0; i < partition_num; i++) {
            if(partition_bitmap[i] == 1) {
                partition_info.push_back(i);
                parallelism_count++;
            }
        }
        for(int i = 0; i < parallelism_count - 1; i++) {
            auto pipeline = pipelines[0]->Copy(partition_info[i]);
            if(!is_root) {
                pipeline->parent = dependencies[0]->pipelines[i+1];
                dependencies[0]->pipelines[i+1]->child = pipeline;
            }
            this->add_pipeline(pipeline);
        }
        for(int i = 0; i < parallelism_count; i++) {
            pipelines[i]->Initialize(i, partition_info[i]);
        }
    } else {
        parallelism_count = dependencies[0]->parallelism;
        for(int i = 0; i < parallelism_count - 1; i++) {
            int work_id = dependencies[0]->pipelines[i+1]->work_id;
            auto pipeline = pipelines[0]->Copy(work_id);
            pipeline->parent = dependencies[0]->pipelines[i+1];
            dependencies[0]->pipelines[i+1]->child = pipeline;
            this->add_pipeline(pipeline);
        }
        for(int i = 0; i < parallelism_count; i++) {
            pipelines[i]->Initialize(i);
        }
    }
    //     Step3: 对于一些必须要维护全局Sink状态的算子（例如，OrderBy），可以在这里配置
    //     TODO: 后续如果有其它需要配置全局状态的算子，最好修改为switch-case的判定方式
    if(pipelines[0]->sink && pipelines[0]->sink->type == PhysicalOperatorType::ORDER_BY) {
        gstate = pipelines[0]->sink->GetGlobalSinkState();
        auto &global_state = (*gstate).Cast<OrderGlobalSinkState>();
        global_state.local_cache.resize(parallelism_count);
        global_state.local_sort_idx.resize(parallelism_count);
        for(int i = 0; i < parallelism_count; i++) {
            pipelines[i]->sink->gsink_state = gstate;
        }
    }
}

std::vector<std::shared_ptr<Pipeline>> PipelineGroup::get_pipelines() {
    return pipelines;
}

void PipelineGroup::add_pipeline(const std::shared_ptr<Pipeline> &pipeline) {
    pipelines.push_back(pipeline);
    pipeline->pipelines = this->getptr();
    pipeline->pipeline_id = parallelism;
    parallelism++;
} // add_pipeline

void PipelineGroup::add_dependence(std::shared_ptr<PipelineGroup> dependence) {
    dependencies.push_back(dependence);
}

void PipelineGroup::create_child_group(std::shared_ptr<PhysicalOperator> &current) {
    auto operator_type = current->type;
    if(current->children_operator.size() > 1) {
        switch (operator_type) {
            case PhysicalOperatorType::RADIX_JOIN:
            {
                // Probe
                auto &child_operator_probe = current->children_operator[1];
                auto child_pipeline_group_probe = std::make_shared<PipelineGroup>();
                auto child_pipeline_probe = std::make_shared<Pipeline>();
                std::shared_ptr<ExecuteContext> child_execute_context_probe = std::make_shared<ExecuteContext>();
                child_execute_context_probe->set_pipeline(child_pipeline_probe);
                child_pipeline_group_probe->add_pipeline(child_pipeline_probe);
                dependencies.push_back(child_pipeline_group_probe);
                child_pipeline_probe->sink = current;
                child_pipeline_probe->child = pipelines[0];
                pipelines[0]->parent = child_pipeline_probe;
                child_operator_probe->build_pipelines(child_pipeline_probe, child_pipeline_group_probe);
                // Build
                auto &child_operator_build = current->children_operator[0];
                auto child_pipeline_group_build = std::make_shared<PipelineGroup>();
                auto child_pipeline_build = std::make_shared<Pipeline>();
                std::shared_ptr<ExecuteContext> child_execute_context_build = std::make_shared<ExecuteContext>();
                child_execute_context_build->set_pipeline(child_pipeline_build);
                child_pipeline_group_build->add_pipeline(child_pipeline_build);
                child_pipeline_group_probe->dependencies.push_back(child_pipeline_group_build);
                child_pipeline_build->sink = current;
                child_pipeline_build->child = child_pipeline_probe;
                child_pipeline_probe->parent = child_pipeline_build;
                child_operator_build->build_pipelines(child_pipeline_build, child_pipeline_group_build);
                break;
            }
            case PhysicalOperatorType::NESTED_LOOP_JOIN:
            case PhysicalOperatorType::HASH_JOIN:
            {
                // Build
                auto &child_operator = current->children_operator[0];
                auto child_pipeline_group = std::make_shared<PipelineGroup>();
                auto child_pipeline = std::make_shared<Pipeline>();
                std::shared_ptr<ExecuteContext> child_execute_context = std::make_shared<ExecuteContext>();
                child_execute_context->set_pipeline(child_pipeline);
                child_pipeline_group->add_pipeline(child_pipeline);
                dependencies.push_back(child_pipeline_group);
                child_pipeline->sink = current;
                child_pipeline->child = pipelines[0];
                if(pipelines[0]->parent == nullptr) {
                    pipelines[0]->parent = child_pipeline;
                }
                child_operator->build_pipelines(child_pipeline, child_pipeline_group);
                break;
            }
            default:
                break;
        } // switch
    } else {
        auto &child_operator = current->children_operator[0];
        auto child_pipeline_group = std::make_shared<PipelineGroup>();
        auto child_pipeline = std::make_shared<Pipeline>();
        std::shared_ptr<ExecuteContext> child_execute_context = std::make_shared<ExecuteContext>();
        child_execute_context->set_pipeline(child_pipeline);
        child_pipeline_group->add_pipeline(child_pipeline);
        dependencies.push_back(child_pipeline_group);
        child_pipeline->sink = current;
        child_pipeline->child = pipelines[0];
        pipelines[0]->parent = child_pipeline;
        child_operator->build_pipelines(child_pipeline, child_pipeline_group);
    }

} // create_child_group

std::shared_ptr<Barrier> PipelineGroup::get_barrier() {
    std::lock_guard<std::mutex> lock(barrier_mutex);
    if (barrier == nullptr) {
        barrier = std::make_shared<Barrier>(parallelism);
        return barrier;
    } else {
        return barrier;
    }
} // get_barrier


} // namespace DaseX
