#include "abstract_plan.h"
#include "arrow_help.hpp"
#include "binder.h"
#include "bound_in_expression.hpp"
#include "bound_project_function_expression.hpp"
#include "catalog.hpp"
#include "expression.hpp"
#include "expression_executor.hpp"
#include "like.hpp"
#include "logical_type.hpp"
#include "nest_loop_join_plan.h"
#include "numa_help.hpp"
// #include "numeric.hpp"
#include "optimizer.hpp"
#include "physical_filter.hpp"
#include "physical_hash_aggregation.hpp"
#include "physical_hash_join.hpp"
#include "physical_order.hpp"
#include "physical_project.hpp"
#include "physical_result_collector.hpp"
#include "physical_table_scan.hpp"
#include "pipe.h"
#include "pipeline_group_execute.hpp"
#include "pipeline_task.hpp"
#include "planner.hpp"
#include "postgres_parser.hpp"
#include "projection_plan.h"
#include "rc.hpp"
#include "scheduler.hpp"
#include "seq_scan_plan.h"
#include "spdlog/fmt/bundled/core.h"
#include "string_functions.hpp"
#include <arrow/api.h>
#include <cinttypes>
#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <spdlog/spdlog.h>
#include <sstream>
#include <string>

using namespace DaseX;

TEST(PipeTest, BasicJoinTest) {
    duckdb::PostgresParser parse;

    std::string test_ddl_sql_1 = "CREATE TABLE A(id INTEGER, score FLOAT)";
    std::string test_ddl_sql_2 = "CREATE TABLE B(id INTEGER, age INT)";

    std::string test_sql_1 = "SELECT * FROM A, B";
    // insert?

    Binder binder(global_catalog);
    binder.ParseAndSave(test_ddl_sql_1);
    binder.ParseAndSave(test_ddl_sql_2);
    binder.ParseAndSave(test_sql_1);
    for (auto *stmt : binder.statement_nodes_) {

        auto statement = binder.BindStatement(stmt);

        switch (statement->type_) {
            case StatementType::CREATE_STATEMENT: {
            const auto &create_stmt =
                dynamic_cast<const CreateStatement &>(*statement);
            RC rc = binder.ExecuteDDL(create_stmt);
            ASSERT_EQ(RC::SUCCESS, rc);
            continue;
            }
            case StatementType::SELECT_STATEMENT:{
                Planner planner(global_catalog);
                const auto &select_stmt =
                        dynamic_cast<const SelectStatement &>(*statement);

                planner.PlanQuery(select_stmt);
                std::cout << planner.plan_->PrintTree() << '\n';
                Optimizer optimizer(binder);
                AbstractPlanNodeRef plan = optimizer.Optimize(planner.plan_);
                std::cout << plan->PrintTree() << '\n';
                // AbstractPlanNodeRef plan = planner.plan_;
                auto pipe = Pipe(72);
                auto physical_ops = pipe.BuildPipe(plan);
                printf("plan end\n");
            }
        }
    }
    binder.statement_nodes_.clear();
}