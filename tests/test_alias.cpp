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
#include <arrow/api.h>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <cinttypes>
#include <cstdio>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

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
#include "import_data.hpp"

using namespace DaseX;

const int arr[] = {1, 2, 3, 4};
const std::string fileDir4 = "/home/lyf/data/experiment_0.1/thread4";
const std::string fileDir1 = "/home/lyf/data/experiment_0.1";

TEST(AliasTest, AliasBasicTest) {
	duckdb::PostgresParser parse;
	
	std::string test_ddl_sql_1 = "CREATE TABLE A(id INTEGER, score FLOAT)";
	std::string test_ddl_sql_2 = "CREATE TABLE B(id INTEGER, age INT)";

    std::vector<std::string> test_sqls = {
        "SELECT A.score as a_score, B.age as b_age FROM A, B WHERE A.id = B.id",
        "SELECT a_score FROM (SELECT A.score as a_score FROM A)",
        "SELECT a_score FROM (SELECT A.score as a_score FROM A) as temp_table",
        "SELECT temp_table.a_score FROM (SELECT A.score as a_score FROM A) as temp_table",
        "SELECT temp_table.a1_score, a2_id FROM (SELECT a1.score as a1_score, a2.id as a2_id FROM A a1, A a2 WHERE a1.id = a2.id) as temp_table",
		"SELECT temp_table.a1_score_max, a2_id FROM (SELECT max(a1.score) as a1_score_max, a2.id as a2_id FROM A a1, A a2 WHERE a1.id = a2.id GROUP BY a2.id) as temp_table"
    };

	Binder binder(global_catalog);
	binder.ParseAndSave(test_ddl_sql_1);
	binder.ParseAndSave(test_ddl_sql_2);
    for (auto &sql : test_sqls) {
        binder.ParseAndSave(sql); 
    }
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
			case StatementType::SELECT_STATEMENT: {
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

// TEST(AliasTest, AliasTPCHTestQ7FourThread) {
// 	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
//     bool importFinish = false;
//     std::string fileDir = fileDir4;
//     std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
//     std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
//     // // 插入Supplier表数据
//     // std::shared_ptr<Table> table_supplier;
//     // std::vector<std::string> supplier_file_names;
//     // for(int i = 0; i < work_ids.size(); i++) {
//     //     std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
//     //     supplier_file_names.emplace_back(file_name);
//     // }
//     // InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
//     // importFinish = false;
//     // std::this_thread::sleep_for(std::chrono::seconds(1));

//     // // 插入Lineitem表数据
//     std::shared_ptr<Table> table_lineitem;
//     std::vector<std::string> lineitem_file_names;
//     for(int i = 0; i < work_ids.size(); i++) {
//         std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
//         lineitem_file_names.emplace_back(file_name);
//     }
//     InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
//     importFinish = false;
//     std::this_thread::sleep_for(std::chrono::seconds(2));
//     spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

//     // 插入Nation表数据
//     std::shared_ptr<Table> table_nation;
//     std::vector<std::string> nation_file_names;
//     for(int i = 0; i < work_ids.size(); i++) {
//         std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
//         nation_file_names.emplace_back(file_name);
//     }
//     InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
//     importFinish = false;
//     std::this_thread::sleep_for(std::chrono::seconds(1));
//     spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

// 

// 	std::string test_sql_tpch_q7_a = 
// 		"select "
// 			"supp_nation, "
//     		"cust_nation "
// 			"l_year, sum(volume) as revenue "
// 		"from ( "
// 		"select "
// 			"n1.n_name as supp_nation, "
// 			"n2.n_name as cust_nation, "
// 			"l_shipdate as l_year, "
// 			"l_extendedprice * (1 - l_discount) as volume "
// 			"from "
// 			"lineitem, "
// 			"nation n1, "
// 			"nation n2 "
// 			"where "
// 			"n1.n_nationkey = n2.n_nationkey "
//             "and lineitem.l_orderkey = n1.n_nationkey"
// 		") as shipping";
			

//     std::string test_sql_tpch_q7_b = 
// 		"select "
// 			"shipping.supp_nation, "
//     		"shipping.cust_nation "
// 			"l_year, sum(volume) as revenue "
// 		"from ( "
// 		"select "
// 			"n1.n_name as supp_nation, "
// 			"n2.n_name as cust_nation, "
// 			"l_shipdate as l_year, "
// 			"l_extendedprice * (1 - l_discount) as volume "
// 			"from "
// 			"lineitem, "
// 			"nation n1, "
// 			"nation n2 "
// 			"where "
// 			"n1.n_nationkey = n2.n_nationkey "
//             "and lineitem.l_orderkey = n1.n_nationkey"
// 		") as shipping";



// 	Binder binder(*catalog);
// 	// binder.ParseAndSave(test_ddl_sql_1);
// 	binder.ParseAndSave(test_sql_tpch_q7_a);
//     binder.ParseAndSave(test_sql_tpch_q7_b);

// 	for (auto *stmt : binder.statement_nodes_) {
// 		auto statement = binder.BindStatement(stmt);
// 		switch (statement->type_) {
// 			case StatementType::CREATE_STATEMENT: {
// 				const auto &create_stmt =
// 					dynamic_cast<const CreateStatement &>(*statement);
// 				RC rc = binder.ExecuteDDL(create_stmt);
// 				ASSERT_EQ(RC::SUCCESS, rc);
// 				continue;
// 			}
// 			case StatementType::SELECT_STATEMENT: {
// 				Planner planner(*catalog);
// 				const auto &select_stmt =
// 					dynamic_cast<const SelectStatement &>(*statement);

// 				planner.PlanQuery(select_stmt);
// 				std::cout << planner.plan_->PrintTree() << '\n';
// 				auto plan = planner.plan_;
// 				Optimizer optimizer(binder);
// 				plan = optimizer.Optimize(planner.plan_);
// 				// AbstractPlanNodeRef plan = planner.plan_;
// 				std::cout << plan->PrintTree() << '\n';
// 				// AbstractPlanNodeRef plan = planner.plan_;
// 				auto pipe = Pipe(72);
// 				auto physical_ops = pipe.BuildPipe(plan);
// 				// 如果 project 算子在最顶层，则加一个 ResultCollector 算子
// 				// 这个逻辑先只写在测试用例里
// 				if (physical_ops[0]->type == PhysicalOperatorType::PROJECTION) {
// 					auto root = std::make_shared<PhysicalResultCollector>(
// 						physical_ops[0]);
// 					root->children_operator = {physical_ops[0]};
// 					physical_ops[0] = std::move(root);
// 				}
// 				printf("plan end\n");


// 				std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
//     			std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
// 				physical_ops[0]->build_pipelines(pipeline, pipeline_group);
// 				std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
// 				pipeline_group_executor->traverse_plan();
// 				auto start_execute = std::chrono::steady_clock::now();
// 				pipeline_group_executor->execute();
// 				auto end_execute = std::chrono::steady_clock::now();
// 				spdlog::info("[{} : {}] Q2执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
// 				scheduler->shutdown();
// 				RC expected = RC::SUCCESS;
// 				ASSERT_EQ(expected, RC::SUCCESS);
// 			}
// 		}
// 	}
// 	// binder.statement_nodes_.clear();
// }
