#include "physical_operator.hpp"
#include "pipeline.hpp"

namespace DaseX {

// common
void PhysicalOperator::build_pipelines(std::shared_ptr<Pipeline> &current,
                                       std::shared_ptr<PipelineGroup> &pipelineGroup) {
    if (is_sink()) {
        // operator is a sink, build a pipeline,
        // it becomes the data source of the current pipeline
        current->source = this->getptr();
        if(pipelineGroup->pipelines.empty()) {
            std::shared_ptr<ExecuteContext> execute_context_base = std::make_shared<ExecuteContext>();
            execute_context_base->set_pipeline(current);
            pipelineGroup->add_pipeline(current);
        }
//        std::shared_ptr<ExecuteContext> execute_context_base = std::make_shared<ExecuteContext>();
//        execute_context_base->set_pipeline(current);
//        pipelineGroup->add_pipeline(current);
        // we create a new pipelineGroup starting from the child
        pipelineGroup->create_child_group(current->source);
    } else {
        // operator is not a sink! recurse in children
        if (children_operator.empty()) {
            // source
            current->source = this->getptr();
        } else {
            if (children_operator.size() != 1) {
                throw std::runtime_error("Operator not supported in BuildPipelines");
            }
            current->operators.push_back(this->getptr());
            children_operator[0]->build_pipelines(current, pipelineGroup);
        }
    }
}

void PhysicalOperator::AddChild(std::shared_ptr<PhysicalOperator> child) {
    children_operator.emplace_back(child);
}
// Source interface
std::shared_ptr<LocalSourceState> PhysicalOperator::GetLocalSourceState() const {
    return std::make_shared<LocalSourceState>();
}

std::shared_ptr<LocalSinkState> PhysicalOperator::GetLocalSinkState() const {
    return std::make_shared<LocalSinkState>();
}

std::shared_ptr<GlobalSinkState> PhysicalOperator::GetGlobalSinkState() const {
    return std::make_shared<GlobalSinkState>();
}



} // DaseX