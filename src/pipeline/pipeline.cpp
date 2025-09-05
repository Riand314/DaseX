#include "pipeline.hpp"
#include "physical_radix_join.hpp"
#include "physical_hash_join.hpp"
#include "physical_nested_loop_join.hpp"
#include "physical_table_scan.hpp"
#include "physical_hash_aggregation.hpp"
#include "physical_hash_aggregation_with_multifield.hpp"
#include <utility>

namespace DaseX
{

Pipeline::Pipeline() :
                   input(nullptr),
                   output(nullptr),
                   source(nullptr),
                   sink(nullptr),
                   temporary_chunk(nullptr),
                   is_final_chunk(false),
                   is_finish(false) {}

Pipeline::Pipeline(std::shared_ptr<LocalSourceState> input_, std::shared_ptr<LocalSinkState> output_) : input(std::move(input_)),
                                                                         output(std::move(output_)),
                                                                         source(nullptr),
                                                                         sink(nullptr),
                                                                         temporary_chunk(nullptr),
                                                                         is_final_chunk(false),
                                                                         is_finish(false) {}

void Pipeline::add_dependency(std::shared_ptr<Pipeline> parent)
{
    this->parent = parent;
}

void Pipeline::set_child(std::shared_ptr<Pipeline> child) {
    this->child = child;
}

PipelineResultType Pipeline::execute_pipeline()
{
    PipelineResultType result = PipelineResultType::FINISHED;
    SourceResultType source_result = SourceResultType::FINISHED;
    OperatorResultType operator_result = OperatorResultType::FINISHED;
    SinkResultType sink_result = SinkResultType::FINISHED;
    for (uint64_t i = 0; i < max_chunks; i++)
    {
        if (source == nullptr)
        {
            break;
        }
        source_result = source->get_data(execute_context, *input);
        switch (source_result)
        {
        case SourceResultType::HAVE_MORE_OUTPUT:
            result = PipelineResultType::HAVE_MORE_OUTPUT;
            break;
        case SourceResultType::FINISHED:
            result = PipelineResultType::FINISHED;
            this->is_finish = true;
            this->is_final_chunk = true;
            break;
        case SourceResultType::BLOCKED:
            result = PipelineResultType::BLOCKED;
            break;
        default:
            break;
        }
        if (operators.size() != 0)
        {
            //  for (int i = 0; i < operators.size(); i++)
            for (int i = (operators.size() - 1); i >= 0; i--)
            {
                execute_context->operator_idx = i;
                operator_result = operators[i]->execute_operator(execute_context);
                switch (operator_result)
                {
                case OperatorResultType::HAVE_MORE_OUTPUT:
                    result = (source_result == SourceResultType::FINISHED) ? PipelineResultType::FINISHED : PipelineResultType::HAVE_MORE_OUTPUT;
                    break;
                case OperatorResultType::NO_MORE_OUTPUT:
                    result = (source_result == SourceResultType::FINISHED) ? PipelineResultType::FINISHED : PipelineResultType::NO_MORE_OUTPUT;
                    break;
                case OperatorResultType::FINISHED:
                    result = PipelineResultType::FINISHED;
                    break;
                case OperatorResultType::BLOCKED:
                    // TODO something
                    result = PipelineResultType::BLOCKED;
                    break;
                default:
                    break;
                }
                if (operator_result == OperatorResultType::NO_MORE_OUTPUT)
                    break;
            } // for
        }     // operators
        if (operator_result == OperatorResultType::NO_MORE_OUTPUT && source_result != SourceResultType::FINISHED)
        {
            continue;
        }
        if(sink) {
            sink_result = sink->sink(execute_context, *output);
            switch (sink_result)
            {
                case SinkResultType::NEED_MORE_INPUT:
                    result = (source_result == SourceResultType::FINISHED) ? PipelineResultType::FINISHED : PipelineResultType::NEED_MORE_INPUT;
                    break;
                case SinkResultType::FINISHED:
                    result = PipelineResultType::FINISHED;
                    break;
                case SinkResultType::BLOCKED:
                    // TODO something
                    result = PipelineResultType::BLOCKED;
                    break;
                default:
                    break;
            }
        }
        if (result == PipelineResultType::FINISHED)
        {
            auto pipeline_group = this->get_pipeline_group();
            if (auto pipe_group = pipeline_group.lock())
            {
                pipe_group->add_count();
            }
            else
            {
                throw std::runtime_error("Invalid PhysicalRadixJoin::pipeline");
            }
            break;
        }
    } // for
    return result;
} // execute_pipeline

void Pipeline::Initialize(int pipe_idx, int work_id) {
    // Step1: 设置并行度
    // 如果是根pipeline，根据传入的work_id来设置
    // 如果不是，只需要从依赖的pipeline中获取work_id即可
    // TODO:目前所有pipeline_task有统一的并行度，如果要支持灵活的并行配置的话，这里的逻辑需要重新修改
    if(unlikely(work_id != -1)) {
        this->work_id = work_id;
    } else {
        this->work_id = this->parent->work_id;
    }
    if(auto weak_pipelines = pipelines.lock()) {
        is_root = weak_pipelines->is_root;
    }
    // Step2: 为Pipeline配置Source，并初始化本地Source状态
    // pipeline一定会有source
    if(source->type == PhysicalOperatorType::TABLE_SCAN) {
        auto &lsource = (*source).Cast<PhysicalTableScan>();
        lsource.partition_id = work_id;
        input = lsource.GetLocalSourceState();
    } else {
        // Copy Pipeline主要初始化Source,在这里就可以设置lsink_state
        source->lsink_state = parent->output;
        input = source->GetLocalSourceState();
    }
    // Step3: 初始化Join的状态
    int count_pg = 0;
    int operators_size = operators.size();
    for(int i = (operators_size - 1) ; i >=0 ; i--) {
        switch (operators[i]->type) {
            case PhysicalOperatorType::HASH_JOIN:
            {
                auto &join_op = (*operators[i]).Cast<PhysicalHashJoin>();
                if(auto weak_pipelines = pipelines.lock()) {
                    auto &depend_pg = weak_pipelines->dependencies;
                    auto &cur_pg = depend_pg[count_pg];
                    auto &cur_pipe = cur_pg->pipelines[pipe_idx];
                    if(cur_pipe->sink->type != PhysicalOperatorType::HASH_JOIN) {
                        count_pg++;
                        auto &next_pg = depend_pg[count_pg];
                        auto &next_pipe = next_pg->pipelines[pipe_idx];
                        join_op.lsink_state = next_pipe->output;
                    } else {
                        join_op.lsink_state = cur_pipe->output;
                    }
                }
                count_pg++;
                break;
            }
            case PhysicalOperatorType::NESTED_LOOP_JOIN:
            {
                auto &join_op = (*operators[i]).Cast<PhysicalNestedLoopJoin>();
                if(auto weak_pipelines = pipelines.lock()) {
                    auto &depend_pg = weak_pipelines->dependencies;
                    auto &cur_pg = depend_pg[count_pg];
                    auto &cur_pipe = cur_pg->pipelines[pipe_idx];
                    if(cur_pipe->sink->type != PhysicalOperatorType::NESTED_LOOP_JOIN) {
                        count_pg++;
                        auto &next_pg = depend_pg[count_pg];
                        auto &next_pipe = next_pg->pipelines[pipe_idx];
                        join_op.lsink_state = next_pipe->output;
                    } else {
                        join_op.lsink_state = cur_pipe->output;
                    }
                }
                count_pg++;
                break;
            }
            default:
                break;
        }
    }
    // Step4: 为Pipeline配置Sink，并初始化本地Sink状态
    // 如果pipeline有sink，就需要为pipeline设置输出
    if(sink) {
        switch (sink->type) {
            case PhysicalOperatorType::RADIX_JOIN:
            {
                if((*sink).Cast<PhysicalRadixJoin>().is_build) {
                    output = this->sink->GetLocalSinkState();
                    sink->lsink_state = output;
                } else {
                    output = this->parent->sink->lsink_state;
                }
                break;
            }
            case PhysicalOperatorType::HASH_GROUP_BY:
            {
                output = this->sink->GetLocalSinkState();
                auto &output_p = (*output).Cast<AggLocalSinkState>();
                int partition_nums = 0;
                if(auto pipe_group = this->pipelines.lock()) {
                    partition_nums = pipe_group->parallelism;
                }
                output_p.partition_nums = partition_nums;
                output_p.partitioned_hash_tables.resize(partition_nums);
                sink->lsink_state = output;
                break;
            }
            case PhysicalOperatorType::MULTI_FIELD_HASH_GROUP_BY:
            {
                output = this->sink->GetLocalSinkState();
                auto &output_p = (*output).Cast<MultiFieldAggLocalSinkState>();
                int partition_nums = 0;
                if(auto pipe_group = this->pipelines.lock()) {
                    partition_nums = pipe_group->parallelism;
                }
                output_p.partition_nums = partition_nums;
                output_p.partition_table->Initialize(partition_nums);
                sink->lsink_state = output;
                break;
            }
            default:
            {
                output = this->sink->GetLocalSinkState();
                sink->lsink_state = output;
                break;
            }
        } // switch
    }
}  // Initialize

std::shared_ptr<Pipeline> Pipeline::Copy(int work_id) {
    auto new_pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<ExecuteContext> new_execute_context = std::make_shared<ExecuteContext>();
    new_pipeline->pipelines = pipelines;
    new_pipeline->source = source->Copy(work_id);
    for(auto &en : operators) {
        new_pipeline->operators.emplace_back(en->Copy(work_id));
    }
    new_pipeline->work_id = work_id;
    new_pipeline->sink = sink ? sink->Copy(work_id) : nullptr;
    new_pipeline->is_root = is_root;
    new_pipeline->is_final_chunk = is_final_chunk;
    new_pipeline->is_finish = is_finish;
    new_execute_context->set_pipeline(new_pipeline);
    return new_pipeline;
}

} // DaseX
