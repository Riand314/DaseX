//
// Created by zyy on 24-12-23.
//
//
// Created by zyy on 24-11-28.
//
#include <cstdio>
#include <gtest/gtest.h>
#include "pipeline_task.hpp"
#include "scheduler.hpp"
#include <thread>
#include <arrow/api.h>
#include <memory>
#include <string>
#include "arrow_help.hpp"
#include "binder.h"
#include "catalog.hpp"
#include "nest_loop_join_plan.h"
#include "create_statement.hpp"
#include "postgres_parser.hpp"
#include "binder.h"
#include "spdlog/fmt/bundled/core.h"
#include "planner.hpp"
#include "statement_type.hpp"
#include "select_statement.hpp"
#include "physical_radix_join.hpp"
#include "physical_hash_aggregation.hpp"
#include "pipeline_group_execute.hpp"
#include "pipe.h"
#include "rc.hpp"
#include "fstream"
#include "import_data.hpp"
#include "file_dir_config.hpp"
#define CHUNK_SZIE 4096

using namespace DaseX;


TEST(PlannerTest, TPCH_Q3Test) {
	duckdb::PostgresParser parse;

	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);

	// ================================== 插入数据 ====================================

	// 插入Lineitem表数据
	bool importFinish = false;
	std::shared_ptr<Table> table_lineitem;
	std::vector<int> work_ids = {1, 2, 3, 4};
	std::vector<int> partition_idxs = {1, 2, 3, 4};
	std::vector<std::string> file_names;
	for(int i = 0; i < work_ids.size(); i++) {
		std::string file_name = fileDir4 + "/" + "lineitem.tbl_" + std::to_string(i);
		file_names.emplace_back(file_name);
	}
	InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, file_names,importFinish);
	importFinish = false;
	std::this_thread::sleep_for(std::chrono::seconds(2));
	spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);


	// 插入Orders表数据
	std::shared_ptr<Table> table_orders;
	std::vector<std::string> orders_file_names;
	for(int i = 0; i < work_ids.size(); i++) {
		std::string file_name = fileDir4 + "/" + "orders.tbl_" + std::to_string(i);
		orders_file_names.emplace_back(file_name);
	}
	InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
	importFinish = false;
	std::this_thread::sleep_for(std::chrono::seconds(2));
	spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);


	// 插入Customer表数据
	std::shared_ptr<Table> table_customer;
	std::vector<std::string> customer_file_names;
	for(int i = 0; i < work_ids.size(); i++) {
		std::string file_name = fileDir4 + "/" + "customer.tbl_" + std::to_string(i);
		customer_file_names.emplace_back(file_name);
	}
	InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
	importFinish = false;


	// ============================================================================

	// ================================== 执行SQL ====================================

	std::string tpch_Q3 = "select\n"
		"l_orderkey,\n"
		"sum(l_extendedprice*(1.0-l_discount)) as revenue, \n"
		"o_orderdate,\n"
		"o_shippriority\n"
		"from\n"
		"customer, orders, lineitem \n"
		"where\n"
		"c_mktsegment = 'BUILDING' \n"
		"and c_custkey = o_custkey\n"
		"and l_orderkey = o_orderkey\n"
		"and o_orderdate < 795196800 \n"
		"and l_shipdate > 795196800\n"
		"group by \n"
		"l_orderkey, \n"
		"o_orderdate,\n"
		"o_shippriority\n"
		"order by\n"
		//"revenue desc, \n"
		"o_orderdate;";

	// 创建 binder，绑定 catalog

	Binder binder(global_catalog);
	// 以下两句用于 CREATE 语句的测试，因插入数据函数中自带表信息，在此注释。
	// binder.ParseAndSave(test_ddl_sql_1);
	// binder.ParseAndSave(test_ddl_sql_2);
	auto start_parse = std::chrono::steady_clock::now();
	// 调用 pg 解析器进行词法语法解析
	binder.ParseAndSave(tpch_Q3);
	auto stmt = binder.statement_nodes_[0];
	// binder 阶段，完成 PG AST 中信息与本数据库元数据的绑定
	auto statement = binder.BindStatement(stmt);
	// 创建 planner ，绑定 catalog
	Planner planner(global_catalog);
	// 根据 binder 结果，生成逻辑计划树
	const auto &select_stmt =
		dynamic_cast<const SelectStatement &>(*statement);
	planner.PlanQuery(select_stmt);

	auto pipe = Pipe(72);
	// 将逻辑算子转换为物理算子
	auto physical_ops = pipe.BuildPipe(planner.plan_);
	// 以物理计划树根算子建立pipeline和pipeline_group
	std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
	std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
	physical_ops[0]->build_pipelines(pipeline,pipeline_group);
	std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
	pipeline_group_executor->traverse_plan();
	auto start_execute = std::chrono::steady_clock::now();
	// 执行SQL
	pipeline_group_executor->execute();
	auto end_execute = std::chrono::steady_clock::now();
	spdlog::info("[{} : {}] Q3解析时间: {}", __FILE__, __LINE__, Util::time_difference(start_parse, start_execute));
	spdlog::info("[{} : {}] Q3执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
	scheduler->shutdown();

	binder.statement_nodes_.clear();
	scheduler->shutdown();
}
