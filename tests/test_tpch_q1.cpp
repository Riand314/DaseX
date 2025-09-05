

//
// Created by zyy on 24-11-25.
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


TEST(PlannerTest, TPCH_Q1Test) {
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


    // 插入Part表数据
    std::shared_ptr<Table> table_region;
    std::vector<std::string> file_names_2;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir4 + "/" + "part.tbl_" + std::to_string(i);
        file_names_2.emplace_back(file_name);
    }
    InsertPartMul(table_region, scheduler, work_ids,partition_idxs,file_names_2, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] Insertpart Finish!!!", __FILE__, __LINE__);

    // ============================================================================

    // ================================== 执行SQL ====================================
    std::string test_ddl_sql_1 =
            "CREATE TABLE Lineitem (l_orderkey int,"
            "l_partkey int, l_suppkey "
            "int, l_extendedprice  int, l_linenumber int,l_quantity "
            "int,l_discount int,l_tax "
            "int,l_returnflag string,l_linestatus "
            "int,l_shipdate int,l_commitdate int,l_shipmode "
            "int,l_comment int, l_shipinstruct int,l_receiptdate int);";

    std::string  test_ddl_sql_2 = "CREATE TABLE Orders (\n"
                                  "            o_orderkey       int,\n"
                                  "            o_custkey        int,\n"
                                  "            o_orderstatus    string,\n"
                                  "            o_totalprice     int,\n"
                                  "            o_orderdate      int,\n"
                                  "            o_orderpriority  int,\n"
                                  "            o_clerk          int,\n"
                                  "            o_shippriority   int,\n"
                                  "            o_comment        string\n"
                                  "    );";


    std::string tpch_Q1 = "select\n"
                          "        l_returnflag,\n"
                          "        l_linestatus,\n"
                          "        sum(l_quantity) as sum_qty,\n"
                          "        sum(l_extendedprice) as sum_base_price,\n"
                          "        sum(l_extendedprice * (1.0 + l_discount)) as sum_disc_price,\n"
                          "        sum(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) as sum_charge,\n"
                          "        avg(l_quantity) as avg_qty,\n"
                          "        avg(l_extendedprice) as avg_price,\n"
                          "        avg(l_discount) as avg_disc,\n"
                          "        count(*) as count_order\n"
                          "from\n"
                          "        lineitem\n"
                          "where\n"
                          "        l_shipdate <= 875635200\n"
                          "group by\n"
                          "        l_returnflag,\n"
                          "        l_linestatus\n"
                          "order by\n"
                          "        l_returnflag,\n"
                          "        l_linestatus;";


    // 创建 binder，绑定 catalog
    Binder binder(global_catalog);
    // 以下两句用于 CREATE 语句的测试，因插入数据函数中自带表信息，在此注释。
    // binder.ParseAndSave(test_ddl_sql_1);
    // binder.ParseAndSave(test_ddl_sql_2);
    auto start_parse = std::chrono::steady_clock::now();
    // 调用 pg 解析器进行词法语法解析
    binder.ParseAndSave(tpch_Q1);
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
    spdlog::info("[{} : {}] Q1解析时间: {}", __FILE__, __LINE__, Util::time_difference(start_parse, start_execute));
    spdlog::info("[{} : {}] Q1执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
    scheduler->shutdown();

    binder.statement_nodes_.clear();
    scheduler->shutdown();
}
