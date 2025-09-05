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

TEST(OptimizerTest, OptimizerBasicTest) {
	duckdb::PostgresParser parse;
	
	std::string test_ddl_sql_1 = "CREATE TABLE A(id INTEGER, score FLOAT)";
	std::string test_ddl_sql_2 = "CREATE TABLE B(id INTEGER, age INT)";

	std::string test_sql_1 =
		"SELECT A.score, B.age FROM A, B WHERE B.age <= 22 AND A.id=10";


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

TEST(OptimizerTest, OptimizerBasicTest2) {
	duckdb::PostgresParser parse;

	std::string test_ddl_sql_1 = "CREATE TABLE T1(id INTEGER, score FLOAT)";
	std::string test_ddl_sql_2 = "CREATE TABLE T2(id INTEGER, age INTEGER)";
	std::string test_ddl_sql_3 = "CREATE TABLE T3(id INTEGER, price INTEGER)";

	std::string test_sql_1 = "SELECT T1.id FROM T1 WHERE T1.id < 100";


	Binder binder(global_catalog);
	binder.ParseAndSave(test_ddl_sql_1);
	binder.ParseAndSave(test_ddl_sql_2);
	binder.ParseAndSave(test_ddl_sql_3);
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

TEST(OptimizerTest, OptimizerAggTest) {
	duckdb::PostgresParser parse;

	std::string test_ddl_sql_1 = "CREATE TABLE T4(id INTEGER, score FLOAT)";

	std::string test_sql_2 =
		"SELECT sum(score) FROM T4 HAVING sum(score) < 100";
	// group by fail
	// std::string test_sql_2 = "SELECT sum(score) FROM T1 HAVING sum(score) <
	// 100 group by id";

	Binder binder(global_catalog);
	binder.ParseAndSave(test_ddl_sql_1);
	binder.ParseAndSave(test_sql_2);

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

TEST(OptimizerTest, OptimizerTPCHTestQ1FailTest) {
	duckdb::PostgresParser parse;

	std::string test_ddl_sql_1 =
		"CREATE TABLE Lineitem (l_orderkey int,"
		"l_partkey int, l_suppkey "
		"int, l_extendedprice  double, l_linenumber int,l_quantity "
		"float,l_discount float,l_tax "
		"float,l_returnflag string,l_linestatus "
		"string,l_shipdate int,l_commitdate int,l_shipmode "
		"string,l_comment string, l_shipinstruct string,l_receiptdate string);";

	std::string test_ddl_sql_2 =
		"CREATE TABLE Region ( r_regionkey int, r_name "
		"string, r_comment string);";

	//  TEST 聚合内表达式 | PASS
	std::string test_sql_1 =
		"select count(*), sum(l_discount) from lineitem,Region where "
		"l_orderkey > 1;";

	// // TEST semi join \ group by \ order by  | PASS
	std::string test_sql_2 =
		"select l_orderkey from lineitem semi join region on "
		"lineitem.l_orderkey = region.r_regionkey group by l_orderkey, l_tax "
		"order by "
		"r_regionkey ";

	std::string test_sql_tpch_q1_sub1 = 
		"select "
		"avg(l_quantity) as avg_qty, "
		"from "
		"lineitem";

	std::string test_sql_tpch_q1_sub2 = 
		"select "
		"l_extendedprice*(1-l_discount) "
		"from "
		"lineitem";

	// bad case#1
	std::string test_sql_tpch_q1_sub3 = 
		"select " 
		"1-l_discount "
		"from "
		"lineitem";

	// bad case#2
	std::string test_sql_tpch_q1_sub4 = 
		"select "
		"l_discount "
		"from "
		"lineitem "
		"where "
		"l_shipdate <= date'1998-12-01' - interval '90' day";

	// bad case#2
	std::string test_sql_tpch_q1_sub5 = 
		"select "
		"sum(l_discount) "
		"from "
		"lineitem "
		"group by "
			"l_returnflag, l_linestatus ";

	std::string test_sql_tpch_q1 = 
		"select "
		"l_returnflag, "
		"l_linestatus, "
		"sum(l_quantity) as sum_qty, "
		"sum(l_extendedprice) as sum_base_price,  "
		// "sum(l_extendedprice*(1-l_discount)) as sum_disc_price,  "
		// "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,  "
		"avg(l_quantity) as avg_qty, "
		"avg(l_extendedprice) as avg_price, "
		"avg(l_discount) as avg_disc, "
		"count(*) as count_order "
		"from "
			"lineitem "
		"where "
			"l_shipdate <= 888888888 "
		"group by  "
			"l_returnflag, l_linestatus "
		"order by "
			"l_returnflag, l_linestatus;";
	

	Binder binder(global_catalog);
	binder.ParseAndSave(test_ddl_sql_1);
	binder.ParseAndSave(test_sql_tpch_q1_sub3);
	// binder.ParseAndSave(test_sql_tpch_q1_sub4);

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

TEST(OptimizerTest, OptimizerTPCHTestQ1) {
	

	duckdb::PostgresParser parse;

	// std::string test_ddl_sql_1 =
	// 	"CREATE TABLE Lineitem (l_orderkey int,"
	// 	"l_partkey int, l_suppkey "
	// 	"int, l_extendedprice  double, l_linenumber int,l_quantity "
	// 	"float,l_discount float,l_tax "
	// 	"float,l_returnflag string,l_linestatus "
	// 	"string,l_shipdate int,l_commitdate int,l_shipmode "
	// 	"string,l_comment string, l_shipinstruct string,l_receiptdate string);";

	std::string test_sql_tpch_q1 = 
		"select "
		"l_returnflag, "
		"l_linestatus, "
		"sum(l_quantity) as sum_qty, "
		"sum(l_extendedprice) as sum_base_price,  "
		"avg(l_quantity) as avg_qty, "
		"avg(l_extendedprice) as avg_price, "
		"avg(l_discount) as avg_disc, "
		"count(*) as count_order "
		"from "
			"lineitem "
		"where "
			"l_shipdate <= 875635200 "
		"group by  "
			"l_returnflag, l_linestatus "
		"order by "
			"l_returnflag, l_linestatus;";


	/////
	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    Util::print_socket_free_memory();
    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<int> work_ids = {1, 2, 3, 4};
    std::vector<int> partition_idxs = {1, 2, 3, 4};
    std::vector<std::string> file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = std::string("/home/lyf/data/experiment_0.1/thread4") + "/" + "lineitem.tbl_" + std::to_string(i);
        file_names.emplace_back(file_name);
    }
	bool importFinish = false;
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, file_names, importFinish);
	importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);
	/////
	Binder binder(global_catalog);
	// binder.ParseAndSave(test_ddl_sql_1);
	binder.ParseAndSave(test_sql_tpch_q1);

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
				// AbstractPlanNodeRef plan = planner.plan_;
				std::cout << plan->PrintTree() << '\n';
				// AbstractPlanNodeRef plan = planner.plan_;
				auto pipe = Pipe(72);
				auto physical_ops = pipe.BuildPipe(plan);
				// 如果 project 算子在最顶层，则加一个 ResultCollector 算子
				// 这个逻辑先只写在测试用例里
				if (physical_ops[0]->type == PhysicalOperatorType::PROJECTION) {
					auto root = std::make_shared<PhysicalResultCollector>(
						physical_ops[0]);
					root->children_operator = {physical_ops[0]};
					physical_ops[0] = std::move(root);
				}
				printf("plan end\n");
				std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    			std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
				physical_ops[0]->build_pipelines(pipeline, pipeline_group);
				std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
				pipeline_group_executor->traverse_plan();
				auto start_execute = std::chrono::steady_clock::now();
				pipeline_group_executor->execute();
				auto end_execute = std::chrono::steady_clock::now();
				spdlog::info("[{} : {}] Q1执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
				scheduler->shutdown();
				RC expected = RC::SUCCESS;
				ASSERT_EQ(expected, RC::SUCCESS);
			}
		}
	}
	// binder.statement_nodes_.clear();
}

TEST(OptimizerTest, OptimizerTPCHTestQ2Single) {


	    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    InsertNation(table_nation, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    // 插入Region表数据
    std::shared_ptr<Table> table_region;
    InsertRegion(table_region, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertRegion Finish!!!", __FILE__, __LINE__);

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    InsertPartsupp(table_partsupp, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPartsupp Finish!!!", __FILE__, __LINE__);

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    InsertPart(table_part, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Supplier表数据
   	std::shared_ptr<Table> table_supplier;
    InsertSupplier(table_supplier, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

	duckdb::PostgresParser parse;

	std::string test_sql_tpch_q2 = 
		"select "
			"ps_partkey "
		"from "
			"partsupp "
		"where "
			// "p_partkey = ps_partkey "
			// "and s_suppkey = ps_suppkey "
			// "and p_size = 25 "
			// "and p_type like '%BRASS' "
			// "and s_nationkey = n_nationkey "
			// "and n_regionkey = r_regionkey "
			// "and r_name = 'ASIA' "
			"ps_supplycost = "
			"( "
				"select "
					"ps_supplycost "
				"from "
					"part, partsupp "
				"where "
					"p_partkey = ps_partkey "
			") "
			"; ";

	std::string test_sql_tpch_q2_sub1 = 
		"select "
			"min(ps_supplycost) "
		"from "
			"part, partsupp "
		"where "
			"p_partkey = ps_partkey "
			// "and s_suppkey = ps_suppkey "
			// "and s_nationkey = n_nationkey "
			// "and n_regionkey = r_regionkey "
		"; ";


	Binder binder(global_catalog);
	// binder.ParseAndSave(test_ddl_sql_1);
	binder.ParseAndSave(test_sql_tpch_q2);

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
				auto plan = planner.plan_;
				Optimizer optimizer(binder);
				plan = optimizer.Optimize(planner.plan_);
				// AbstractPlanNodeRef plan = planner.plan_;
				std::cout << plan->PrintTree() << '\n';
				// AbstractPlanNodeRef plan = planner.plan_;
				auto pipe = Pipe(72);
				auto physical_ops = pipe.BuildPipe(plan);
				// 如果 project 算子在最顶层，则加一个 ResultCollector 算子
				// 这个逻辑先只写在测试用例里
				if (physical_ops[0]->type == PhysicalOperatorType::PROJECTION) {
					auto root = std::make_shared<PhysicalResultCollector>(
						physical_ops[0]);
					root->children_operator = {physical_ops[0]};
					physical_ops[0] = std::move(root);
				}
				printf("plan end\n");



				std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    			std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
				physical_ops[0]->build_pipelines(pipeline, pipeline_group);
				std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
				pipeline_group_executor->traverse_plan();
				auto start_execute = std::chrono::steady_clock::now();
				pipeline_group_executor->execute();
				auto end_execute = std::chrono::steady_clock::now();
				spdlog::info("[{} : {}] Q2执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
				scheduler->shutdown();
				RC expected = RC::SUCCESS;
				ASSERT_EQ(expected, RC::SUCCESS);
			}
		}
	}
	// binder.statement_nodes_.clear();
}

TEST(OptimizerTest, OptimizerTPCHTestQ2FourThread) {


	    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    importFinish = false;

    // 插入Region表数据
    std::shared_ptr<Table> table_region;
    std::vector<std::string> region_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "region.tbl_" + std::to_string(i);
        region_file_names.emplace_back(file_name);
    }
    InsertRegionMul(table_region, scheduler, work_ids, partition_idxs, region_file_names, importFinish);
    importFinish = false;

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    std::vector<std::string> partsupp_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "partsupp.tbl_" + std::to_string(i);
        partsupp_file_names.emplace_back(file_name);
    }
    InsertPartsuppMul(table_partsupp, scheduler, work_ids, partition_idxs, partsupp_file_names, importFinish);
    importFinish = false;

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    std::vector<std::string> supplier_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
        supplier_file_names.emplace_back(file_name);
    }
    InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    importFinish = false;


	duckdb::PostgresParser parse;

	std::string test_sql_tpch_q2 = 
		"select "
			"ps_partkey "
		"from "
			"partsupp "
		"where "
			// "p_partkey = ps_partkey "
			// "and s_suppkey = ps_suppkey "
			// "and p_size = 25 "
			// "and p_type like '%BRASS' "
			// "and s_nationkey = n_nationkey "
			// "and n_regionkey = r_regionkey "
			// "and r_name = 'ASIA' "
			"ps_supplycost = "
			"( "
				"select "
					"ps_supplycost "
				"from "
					"part, partsupp "
				"where "
					"p_partkey = ps_partkey "
			") "
			"; ";
		// "order by "
		// "s_acctbal desc, "
		// "n_name, "
		// "s_name, "
		// "p_partkey; ";

	std::string test_sql_tpch_q2_sub1 = 
		"select "
			"min(ps_supplycost) "
		"from "
			"part, partsupp "
		"where "
			"p_partkey = ps_partkey "
			// "and s_suppkey = ps_suppkey "
			// "and s_nationkey = n_nationkey "
			// "and n_regionkey = r_regionkey "
		"; ";


	Binder binder(global_catalog);
	// binder.ParseAndSave(test_ddl_sql_1);
	binder.ParseAndSave(test_sql_tpch_q2);

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
				auto plan = planner.plan_;
				Optimizer optimizer(binder);
				plan = optimizer.Optimize(planner.plan_);
				// AbstractPlanNodeRef plan = planner.plan_;
				std::cout << plan->PrintTree() << '\n';
				// AbstractPlanNodeRef plan = planner.plan_;
				auto pipe = Pipe(72);
				auto physical_ops = pipe.BuildPipe(plan);
				// 如果 project 算子在最顶层，则加一个 ResultCollector 算子
				// 这个逻辑先只写在测试用例里
				if (physical_ops[0]->type == PhysicalOperatorType::PROJECTION) {
					auto root = std::make_shared<PhysicalResultCollector>(
						physical_ops[0]);
					root->children_operator = {physical_ops[0]};
					physical_ops[0] = std::move(root);
				}
				printf("plan end\n");


				std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    			std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
				physical_ops[0]->build_pipelines(pipeline, pipeline_group);
				std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
				pipeline_group_executor->traverse_plan();
				auto start_execute = std::chrono::steady_clock::now();
				pipeline_group_executor->execute();
				auto end_execute = std::chrono::steady_clock::now();
				spdlog::info("[{} : {}] Q2执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
				scheduler->shutdown();
				RC expected = RC::SUCCESS;
				ASSERT_EQ(expected, RC::SUCCESS);
			}
		}
	}
	// binder.statement_nodes_.clear();
}


TEST(OptimizerTest, OptimizerTPCHTestQ2FourThreadComplete) {


	    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    importFinish = false;

    // 插入Region表数据
    std::shared_ptr<Table> table_region;
    std::vector<std::string> region_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "region.tbl_" + std::to_string(i);
        region_file_names.emplace_back(file_name);
    }
    InsertRegionMul(table_region, scheduler, work_ids, partition_idxs, region_file_names, importFinish);
    importFinish = false;

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    std::vector<std::string> partsupp_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "partsupp.tbl_" + std::to_string(i);
        partsupp_file_names.emplace_back(file_name);
    }
    InsertPartsuppMul(table_partsupp, scheduler, work_ids, partition_idxs, partsupp_file_names, importFinish);
    importFinish = false;

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    std::vector<std::string> supplier_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
        supplier_file_names.emplace_back(file_name);
    }
    InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    importFinish = false;


	duckdb::PostgresParser parse;

	std::string test_sql_tpch_q2 = 
		"select "
			"s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment "
		"from "
			"part, supplier, partsupp, nation, region "
		"where "
			"p_partkey = ps_partkey "
			"and s_suppkey = ps_suppkey "
			"and p_size = 25 "
			"and p_type like '%BRASS' "
			"and s_nationkey = n_nationkey "
			"and n_regionkey = r_regionkey "
			"and r_name = 'ASIA' "
			"and ps_supplycost = ( "
				"select "
					"min(ps_supplycost) "
				"from "
					"partsupp, supplier, nation, region "
				"where "
					"p_partkey = ps_partkey "
					"and s_suppkey = ps_suppkey "
					"and s_nationkey = n_nationkey "
					"and n_regionkey = r_regionkey "
					"and r_name = 'ASIA' "
			") "
		"order by "
			"s_acctbal desc, "
			"n_name, "
			"s_name, "
			"p_partkey; ";

	std::string test_sql_tpch_q2_mod = 
		"select "
			"s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, subquery.min_cost, subquery.partsupp.ps_partkey "
		"from "
			"part, partsupp, supplier, nation, region, "
			"(select min(ps_supplycost) as min_cost, ps_partkey "
			"from "
					"partsupp, supplier, nation, region "
			"where "
					"s_suppkey = ps_suppkey "
					"and s_nationkey = n_nationkey "
					"and n_regionkey = r_regionkey "
					"and r_name = 'ASIA' "
			"GROUP BY ps_partkey) as subquery "
		"where "
			"p_partkey = ps_partkey "
			"and s_suppkey = ps_suppkey "
			"and p_size = 25 "
			// "and p_type like '%BRASS' "
			"and s_nationkey = n_nationkey "
			"and n_regionkey = r_regionkey "
			"and r_name = 'ASIA' "
			"and ps_partkey = subquery.partsupp.ps_partkey "
		"order by "
			"s_acctbal desc, "
			"n_name, "
			"s_name, "
			"p_partkey; ";

	std::string test_sql_tpch_q2_mod_tiny = 
		"select "
			// "s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, subquery.min_cost, subquery.partsupp.ps_partkey "
			"p_partkey, subquery.min_cost, subquery.partsupp.ps_partkey "
		"from "
			"part, partsupp, " //supplier, nation, region, "
			"(select min(ps_supplycost) as min_cost, ps_partkey "
			"from "
					"partsupp, supplier, nation, region "
			"where "
					"s_suppkey = ps_suppkey "
					"and s_nationkey = n_nationkey "
					"and n_regionkey = r_regionkey "
					// "and r_name = 'ASIA' "
			"GROUP BY ps_partkey) as subquery "
		"where "
			"p_partkey = ps_partkey "
			// "and ps_supplycost = subquery.min_cost "
			"and ps_partkey = subquery.partsupp.ps_partkey ";

		// "order by "
		// 	// "s_acctbal desc, "
		// 	// "n_name, "
		// 	// "s_name, "
		// 	"p_partkey; ";

	std::string test_sub = 
		"select ps_partkey, min(ps_supplycost) as min_cost "
		"from "
			"partsupp, supplier, nation, region "
		"where "
			"s_suppkey = ps_suppkey "
			"and s_nationkey = n_nationkey "
			"and n_regionkey = r_regionkey "
		"GROUP BY ps_partkey";

	// std::string simple_test =
	// 	"select * from partsupp; ";
	// std::string test_sql_tpch_q2_mod_alias = 
	// 	"select "
	// 		"s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, subquery.min_cost, subquery.grouped_key "
	// 	"from "
	// 		"part, supplier, partsupp, nation, region, "
	// 		"(select min(ps_supplycost) as min_cost, ps_partkey as grouped_key "
	// 		"from "
	// 				"partsupp, supplier, nation, region "
	// 		"where "
	// 				// "p_partkey = ps_partkey "
	// 				"s_suppkey = ps_suppkey "
	// 				"and s_nationkey = n_nationkey "
	// 				"and n_regionkey = r_regionkey "
	// 				// "and r_name = 'ASIA' "
	// 		"GROUP BY ps_partkey) as subquery "
	// 	"where "
	// 		"p_partkey = ps_partkey "
	// 		"and s_suppkey = ps_suppkey "
	// 		// "and p_size = 25 "
	// 		// "and p_type like '%BRASS' "
	// 		"and s_nationkey = n_nationkey "
	// 		"and n_regionkey = r_regionkey "
	// 		// "and r_name = 'ASIA' "
	// 		"and ps_partkey = subquery.grouped_key "
	// 		"and ps_supplycost = subquery.min_cost  "
	// 	"order by "
	// 		"s_acctbal desc, "
	// 		"n_name, "
	// 		"s_name, "
	// 		"p_partkey; ";

	Binder binder(global_catalog);
	// binder.ParseAndSave(test_ddl_sql_1);
	binder.ParseAndSave(test_sql_tpch_q2_mod);

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
				auto plan = planner.plan_;
				Optimizer optimizer(binder);
				plan = optimizer.Optimize(planner.plan_);
				// AbstractPlanNodeRef plan = planner.plan_;
				std::cout << plan->PrintTree() << '\n';
				// AbstractPlanNodeRef plan = planner.plan_;
				auto pipe = Pipe(72);
				auto physical_ops = pipe.BuildPipe(plan);
				// 如果 project 算子在最顶层，则加一个 ResultCollector 算子
				// 这个逻辑先只写在测试用例里
				if (physical_ops[0]->type == PhysicalOperatorType::PROJECTION) {
					auto root = std::make_shared<PhysicalResultCollector>(
						physical_ops[0]);
					root->children_operator = {physical_ops[0]};
					physical_ops[0] = std::move(root);
				}
				printf("plan end\n");


				std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    			std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
				physical_ops[0]->build_pipelines(pipeline, pipeline_group);
				std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
				pipeline_group_executor->traverse_plan();
				auto start_execute = std::chrono::steady_clock::now();
				pipeline_group_executor->execute();
				auto end_execute = std::chrono::steady_clock::now();
				spdlog::info("[{} : {}] Q2执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
				scheduler->shutdown();
				RC expected = RC::SUCCESS;
				ASSERT_EQ(expected, RC::SUCCESS);
			}
		}
	}
	// binder.statement_nodes_.clear();
}


TEST(OptimizerTest, OptimizerTPCHTestQ7FourThread) {
	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // // 插入Supplier表数据
    // std::shared_ptr<Table> table_supplier;
    // std::vector<std::string> supplier_file_names;
    // for(int i = 0; i < work_ids.size(); i++) {
    //     std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
    //     supplier_file_names.emplace_back(file_name);
    // }
    // InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    // importFinish = false;
    // std::this_thread::sleep_for(std::chrono::seconds(1));

    // // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);



	std::string test_sql_tpch_q7 = 
		"select "
			"supp_nation, "
    		"cust_nation, "
			"l_year "
		"from ( "
		"select "
			// "n1.n_name as s_nation "
			"n1.n_name as supp_nation, "
			"n2.n_name as cust_nation, "
			"l_shipdate as l_year "
			"from "
			"lineitem, "
			"nation n1, "
			"nation n2 "
			"where "
			"n1.n_nationkey = n2.n_nationkey "
			"and l_orderkey = n1.n_nationkey"
		") as shipping";
			


	Binder binder(global_catalog);
	binder.ParseAndSave(test_sql_tpch_q7);

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
				auto plan = planner.plan_;
				Optimizer optimizer(binder);
				plan = optimizer.Optimize(planner.plan_);
				// AbstractPlanNodeRef plan = planner.plan_;
				std::cout << plan->PrintTree() << '\n';
				// AbstractPlanNodeRef plan = planner.plan_;
				auto pipe = Pipe(72);
				auto physical_ops = pipe.BuildPipe(plan);
				// 如果 project 算子在最顶层，则加一个 ResultCollector 算子
				// 这个逻辑先只写在测试用例里
				if (physical_ops[0]->type == PhysicalOperatorType::PROJECTION) {
					auto root = std::make_shared<PhysicalResultCollector>(
						physical_ops[0]);
					root->children_operator = {physical_ops[0]};
					physical_ops[0] = std::move(root);
				}
				printf("plan end\n");


				std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    			std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
				physical_ops[0]->build_pipelines(pipeline, pipeline_group);
				std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
				pipeline_group_executor->traverse_plan();
				auto start_execute = std::chrono::steady_clock::now();
				pipeline_group_executor->execute();
				auto end_execute = std::chrono::steady_clock::now();
				spdlog::info("[{} : {}] Q2执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
				scheduler->shutdown();
				RC expected = RC::SUCCESS;
				ASSERT_EQ(expected, RC::SUCCESS);
			}
		}
	}
	// binder.statement_nodes_.clear();
}

TEST(OptimizerTest, OptimizerTPCHTestQ14) {
	duckdb::PostgresParser parse;

	std::string lineitem_ddl_sql =
		"CREATE TABLE Lineitem (l_orderkey int,"
		"l_partkey int, l_suppkey "
		"int, l_extendedprice  double, l_linenumber int,l_quantity "
		"float,l_discount float,l_tax "
		"float,l_returnflag string,l_linestatus "
		"string,l_shipdate int,l_commitdate int,l_shipmode "
		"string,l_comment string, l_shipinstruct string,l_receiptdate string);";

	std::string part_ddl_sql =
		"CREATE TABLE part (p_partkey int, "
		"p_name string, p_mfgr string, p_brand string, "
		"p_type string, p_size int, p_container string, "
		"p_retailprice double, p_comment string);";


	std::string promo_revenue_sql =
		"SELECT 100.00 * SUM("
		"CASE WHEN p_type LIKE 'PROMO%' "
		"THEN l_extendedprice * (1 - l_discount) "
		"ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue "
		"FROM lineitem, part "
		"WHERE l_partkey = p_partkey "
		"AND l_shipdate >= 809884800 "
		"AND l_shipdate < 812476800;";



	Binder binder(global_catalog);
	binder.ParseAndSave(lineitem_ddl_sql);
	binder.ParseAndSave(part_ddl_sql);
	binder.ParseAndSave(promo_revenue_sql);

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

TEST(OptimizerTest, OptimizerTPCHTestQ16Threads4) {


	    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    std::vector<std::string> partsupp_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "partsupp.tbl_" + std::to_string(i);
        partsupp_file_names.emplace_back(file_name);
    }
    InsertPartsuppMul(table_partsupp, scheduler, work_ids, partition_idxs, partsupp_file_names, importFinish);
    importFinish = false;

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    std::vector<std::string> supplier_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
        supplier_file_names.emplace_back(file_name);
    }
    InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    importFinish = false;


	duckdb::PostgresParser parse;

	std::string test_sql_tpch = 
		"select "
		"p_brand, "
		"p_type, "
		"p_size, "
		"count(ps_suppkey) as supplier_cnt "
		// "count(*) "
	"from "
		"partsupp, "
		"part "
	"where "
		"p_partkey = ps_partkey "
		// "and p_brand <> 'Brand#45' "
		// "and p_type not like 'MEDIUM POLISHED%' "
		// "and p_size in (49, 14, 23, 45, 19, 3, 36, 9) "
		"and "
		"ps_suppkey in ( "
			"select "
				"s_suppkey "
			"from "
				"supplier "
			"where "
				/// @FIXME: like 会导致执行阶段卡住
				// "s_comment like '%Customer%Complaints%' "
				"s_comment = 'CustomerComplaints' "
		") "
		"";
	// "group by "
	// 	"p_brand, "
	// 	"p_type, "
	// 	"p_size "
	// "order by "
	// 	"supplier_cnt desc, "
	// 	"p_brand, "
	// 	"p_type, "
	// 	"p_size; ";


	Binder binder(global_catalog);
	// binder.ParseAndSave(test_ddl_sql_1);
	binder.ParseAndSave(test_sql_tpch);

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
				auto plan = planner.plan_;
				Optimizer optimizer(binder);
				plan = optimizer.Optimize(planner.plan_);
				// AbstractPlanNodeRef plan = planner.plan_;
				std::cout << plan->PrintTree() << '\n';
				// AbstractPlanNodeRef plan = planner.plan_;
				auto pipe = Pipe(72);
				auto physical_ops = pipe.BuildPipe(plan);
				// 如果 project 算子在最顶层，则加一个 ResultCollector 算子
				// 这个逻辑先只写在测试用例里
				if (physical_ops[0]->type == PhysicalOperatorType::PROJECTION) {
					auto root = std::make_shared<PhysicalResultCollector>(
						physical_ops[0]);
					root->children_operator = {physical_ops[0]};
					physical_ops[0] = std::move(root);
				}
				printf("plan end\n");
				std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    			std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
				physical_ops[0]->build_pipelines(pipeline, pipeline_group);
				std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
				pipeline_group_executor->traverse_plan();
				auto start_execute = std::chrono::steady_clock::now();
				pipeline_group_executor->execute();
				auto end_execute = std::chrono::steady_clock::now();
				spdlog::info("[{} : {}] Q16执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
				scheduler->shutdown();
				RC expected = RC::SUCCESS;
				ASSERT_EQ(expected, RC::SUCCESS);

	// 			std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    // std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    // orderby->build_pipelines(pipeline, pipeline_group);
    // std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    // pipeline_group_executor->traverse_plan();
    // auto start_execute = std::chrono::steady_clock::now();
    // pipeline_group_executor->execute();
    // auto end_execute = std::chrono::steady_clock::now();
    // spdlog::info("[{} : {}] Q2执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // // 关闭线程池
    // scheduler->shutdown();
			}
		}
	}
	// binder.statement_nodes_.clear();
}

TEST(OptimizerTest, OptimizerTPCHTestQ20) {
	duckdb::PostgresParser parse;



	Binder binder(global_catalog);
	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    std::vector<std::string> partsupp_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "partsupp.tbl_" + std::to_string(i);
        partsupp_file_names.emplace_back(file_name);
    }
    InsertPartsuppMul(table_partsupp, scheduler, work_ids, partition_idxs, partsupp_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    spdlog::info("[{} : {}] InsertPartsupp Finish!!!", __FILE__, __LINE__);

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    std::vector<std::string> supplier_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
        supplier_file_names.emplace_back(file_name);
    }
    InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);
	std::string test_sql_tpch_q20 = 
		"select "
			"s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment  "
		"from "
			"part, supplier, partsupp, nation, region "
		"where "
			// "p_partkey = ps_partkey "
			// "and s_suppkey = ps_suppkey "
			// "and p_size = 25 "
			// "and p_type like '%BRASS' "
			// "and s_nationkey = n_nationkey "
			// "and n_regionkey = r_regionkey "
			// "and r_name = 'ASIA' "
			"ps_supplycost = ( "
				"select "
					"min(ps_supplycost) "
				"from "
					"partsupp, supplier, nation, region "
				"where "
					"p_partkey = ps_partkey "
					"and s_suppkey = ps_suppkey "
					"and s_nationkey = n_nationkey "
					"and n_regionkey = r_regionkey "
					"and r_name = 'ASIA' "
			"); ";

		// "order by "
		// 	"s_acctbal desc, "
		// 	"n_name, "
		// 	"s_name, "
		// 	"p_partkey; ";

	// binder.ParseAndSave(lineitem_ddl_sql);
	// binder.ParseAndSave(part_ddl_sql);
	// binder.ParseAndSave(promo_revenue_sql);

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