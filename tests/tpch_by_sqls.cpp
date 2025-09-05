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
#include "query_handler.hpp"

using namespace DaseX;

const int arr[] = {1, 2, 3, 4};
const std::string fileDir4 = "../test_data/4thread";
const std::string fileDir1 = "../test_data";

std::string fileDir = fileDir4;
std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ1) {
	std::string test_sql_tpch_q1 = 
		"select "
		"l_returnflag, "
		"l_linestatus, "
		"sum(l_quantity) as sum_qty, "
		"sum(l_extendedprice) as sum_base_price,  "
        "sum(l_extendedprice*(1-l_discount)) as sum_disc_price, "
        "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, "
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



	open_catalog_ptr = global_catalog;
	/////
	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    Util::print_socket_free_memory();
    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir4 + "/" + "lineitem.tbl_" + std::to_string(i);
        file_names.emplace_back(file_name);
    }
	bool importFinish = false;
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, file_names, importFinish);
	importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);
	//
	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch_q1);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q1执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ2) {

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
            "and p_type like '%BRASS' "
			"and s_nationkey = n_nationkey "
			"and n_regionkey = r_regionkey "
			"and r_name = 'ASIA' "
			"and ps_partkey = subquery.partsupp.ps_partkey "
		"order by "
			"s_acctbal desc, "
			"n_name, "
			"s_name, "
			"p_partkey; ";



	    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
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
	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch_q2_mod);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q2执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ3) {

	std::string test_sql_tpch_q3 = 
		"select "
			"l_orderkey, "
			"sum(l_extendedprice*(1-l_discount)) as revenue, "
			"o_orderdate, "
			"o_shippriority "
		"from "
			"customer, orders, lineitem "
		"where "
			"c_mktsegment = 'BUILDING' "
			"and c_custkey = o_custkey "
			"and l_orderkey = o_orderkey "
			"and o_orderdate < 795196800  "
			"and l_shipdate > 795196800 "
		"group by  "
			"l_orderkey,  "
			"o_orderdate,  "
			"o_shippriority  ";
		/// @FIXME: 修复 alias order by 
		// "order by revenue desc,  "
		// "o_orderdate;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;
	
	// 插入Customer表数据
	std::shared_ptr<Table> table_customer;
	std::vector<std::string> customer_file_names;
	for(int i = 0; i < work_ids.size(); i++) {
		std::string file_name = fileDir + "/" + "customer.tbl_" + std::to_string(i);
		customer_file_names.emplace_back(file_name);
	}
	InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
	importFinish = false;

	// 插入Orders表数据
	std::shared_ptr<Table> table_orders;
	std::vector<std::string> orders_file_names;
	for(int i = 0; i < work_ids.size(); i++) {
		std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
		orders_file_names.emplace_back(file_name);
	}
	InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
	importFinish = false;
	std::this_thread::sleep_for(std::chrono::seconds(1));

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
	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch_q3);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q3执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ4) {

	std::string test_sql_tpch_q4 = 
		"SELECT "
            "o_orderpriority, "
            "COUNT(*) AS order_count "
        "FROM "
            "orders "
        ",( "
            "SELECT l_orderkey, count(*) as l_cnt "
            "FROM lineitem "
            "WHERE l_commitdate < l_receiptdate "
			"GROUP BY l_orderkey"
        ") as q1 "
        "WHERE "
			"o_orderkey = q1.lineitem.l_orderkey "
			"AND q1.l_cnt > 0 "
            "AND o_orderdate >= 741456000 "
            "AND o_orderdate < 749404800 "
        "GROUP BY "
            "o_orderpriority "
        "ORDER BY "
            "o_orderpriority;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

	// 插入Orders表数据
	std::shared_ptr<Table> table_orders;
	std::vector<std::string> orders_file_names;
	for(int i = 0; i < work_ids.size(); i++) {
		std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
		orders_file_names.emplace_back(file_name);
	}
	InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
	importFinish = false;
	std::this_thread::sleep_for(std::chrono::seconds(1));

	// 插入Lineitem表数据
	std::shared_ptr<Table> table_lineitem;
	std::vector<std::string> lineitem_file_names;
	for(int i = 0; i < work_ids.size(); i++) {
		std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
		lineitem_file_names.emplace_back(file_name);
	}
	InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
	importFinish = false;
	std::this_thread::sleep_for(std::chrono::seconds(1));
	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch_q4);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q4执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ5) {

	std::string test_sql_tpch_q5 = 
		"SELECT "
            "n_name, "
            "SUM(l_extendedprice * (1 - l_discount)) AS revenue "
        "FROM "
            "customer, orders, lineitem, supplier, nation, region "
        "WHERE "
            "c_custkey = o_custkey "
            "AND l_orderkey = o_orderkey "
            "AND l_suppkey = s_suppkey "
            "AND c_nationkey = s_nationkey "
            "AND s_nationkey = n_nationkey "
            "AND n_regionkey = r_regionkey "
            "AND r_name = 'ASIA' "
            "AND o_orderdate >= 820425600 "
            "AND o_orderdate < 852048000 "
        "GROUP BY "
            "n_name ";
        // "ORDER BY "
        //     "revenue DESC;";

	// std::string test_sql_tpch_q5 = 
	// "SELECT subquery.n_name, subquery.revenue "
	// "FROM ( "
	// 	"SELECT "
	// 		"n_name, "
	// 		"SUM(l_extendedprice * (1 - l_discount)) AS revenue "
	// 	"FROM "
	// 		"customer, orders, lineitem, supplier, nation, region "
	// 	"WHERE "
	// 		"c_custkey = o_custkey "
	// 		"AND l_orderkey = o_orderkey "
	// 		"AND l_suppkey = s_suppkey "
	// 		"AND c_nationkey = s_nationkey "
	// 		"AND s_nationkey = n_nationkey "
	// 		"AND n_regionkey = r_regionkey "
	// 		"AND r_name = 'ASIA' "
	// 		"AND o_orderdate >= 820425600 "
	// 		"AND o_orderdate < 852048000 "
	// 	"GROUP BY "
	// 		"n_name "
	// ") AS subquery "
	// "ORDER BY "
	// 	"subquery.revenue DESC;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

	// 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation2.tbl_" + std::to_string(i);
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

    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    std::vector<std::string> customer_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "customer.tbl_" + std::to_string(i);
        customer_file_names.emplace_back(file_name);
    }
    InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
    importFinish = false;

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
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
	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch_q5);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ6) {

	std::string test_sql_tpch = 
        "SELECT "
            "sum(l_extendedprice*l_discount) as revenue "
        "FROM "
            "lineitem "
        "WHERE "
            "l_shipdate >= 820425600 "
            "AND l_shipdate < 852048000 "
            "AND l_discount >= 0.03 "
            "AND l_discount <= 0.05 "
            "AND l_quantity < 24;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ7) {

	std::string test_sql_tpch = 
		"SELECT "
			"supp_nation, "
			"cust_nation, "
			// "l_year, "
			"SUM(volume) AS revenue "
		"FROM ( "
			"SELECT "
				"n1.n_name AS supp_nation, "
				"n2.n_name AS cust_nation, "
				// "EXTRACT(YEAR FROM l_shipdate) AS l_year, "
				"l_extendedprice * (1 - l_discount) AS volume "
			"FROM "
				"supplier, lineitem, orders, customer, nation n1, nation n2 "
			"WHERE "
				"s_suppkey = l_suppkey "
				"AND o_orderkey = l_orderkey "
				"AND c_custkey = o_custkey "
				"AND s_nationkey = n1.n_nationkey "
				"AND c_nationkey = n2.n_nationkey "
				"AND ( "
					"(n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') "
					// "OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE') "
				") "
				"AND l_shipdate >= 788889600 AND l_shipdate <= 851961600 "
		") AS shipping ";
		// "GROUP BY "
		// 	"supp_nation, "
		// 	"cust_nation, "
		// 	"l_year "
		// "ORDER BY "
		// 	"supp_nation, "
		// 	"cust_nation, "
		// 	"l_year;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

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

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
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
        std::string file_name = fileDir + "/" + "customer.tbl_" + std::to_string(i);
        customer_file_names.emplace_back(file_name);
    }
    InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ8) {

	std::string test_sql_tpch = 
		"SELECT "
			// "o_year, "
			// "SUM(CASE "
			// 	"WHEN nation = 'BRAZIL' "
			// 	"THEN volume "
			// 	"ELSE 0 "
			// "END) / SUM(volume) AS mkt_share "
			"SUM(volume) AS mkt_share "
		"FROM ( "
			"SELECT "
				// "EXTRACT(YEAR FROM o_orderdate) AS o_year, "
				"l_extendedprice * (1 - l_discount) AS volume, "
				"n2.n_name AS nation "
			"FROM "
				"part, lineitem, supplier, orders, customer, nation n1, nation n2, region "
			"WHERE "
				"p_partkey = l_partkey "
				"AND s_suppkey = l_suppkey "
				"AND l_orderkey = o_orderkey "
				"AND o_custkey = c_custkey "
				"AND c_nationkey = n1.n_nationkey "
				"AND n1.n_regionkey = r_regionkey "
				"AND r_name = 'AMERICA' "
				"AND s_nationkey = n2.n_nationkey "
				"AND o_orderdate >= 788889600 AND o_orderdate <= 851961600 "
				"AND p_type = 'ECONOMY ANODIZED STEEL' "
		") AS all_nations ";
		// "GROUP BY "
		// 	"o_year "
		// "ORDER BY "
		// 	"o_year;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    std::vector<std::string> supplier_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
        supplier_file_names.emplace_back(file_name);
    }
    InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    std::vector<std::string> customer_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "customer.tbl_" + std::to_string(i);
        customer_file_names.emplace_back(file_name);
    }
    InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    };
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Region表数据
    std::shared_ptr<Table> table_region;
    std::vector<std::string> region_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "region.tbl_" + std::to_string(i);
        region_file_names.emplace_back(file_name);
    };
    InsertRegionMul(table_region, scheduler, work_ids, partition_idxs, region_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ9) {
	std::string test_sql_tpch = 
		"SELECT "
			// "nation, "
			// "o_year, "
			"SUM(amount) AS sum_profit "
		"FROM ( "
			"SELECT "
				// "n_name AS nation, "
				// "EXTRACT(YEAR FROM o_orderdate) AS o_year, "
				"l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount "
			"FROM "
				// "part, lineitem, supplier, partsupp, orders, nation "
                "lineitem, part, partsupp "
			"WHERE "
				// "s_suppkey = l_suppkey "
				// "AND ps_suppkey = l_suppkey "
                "ps_suppkey = l_suppkey "
				"AND ps_partkey = l_partkey "
				"AND p_partkey = l_partkey "
				// "AND o_orderkey = l_orderkey "
				// "AND s_nationkey = n_nationkey "
				"AND p_name LIKE '%green%' "
		") ";
		// "GROUP BY "
		// 	"nation "
		// // 	// "o_year "
		// "ORDER BY "
		// 	"nation ";
		// 	"DESC;";
		// "GROUP BY "
		// 	"o_year "
		// "ORDER BY "
		// 	"o_year;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

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

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(3));
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ10) {

	std::string test_sql_tpch = 
        "SELECT "
            "c_custkey, c_name, "
            "SUM(l_extendedprice * (1 - l_discount)) AS revenue, "
            "c_acctbal, "
            "n_name, c_address, c_phone, c_comment "
        "FROM "
            "customer, orders, lineitem, nation "
        "WHERE "
            "c_custkey = o_custkey "
            "AND l_orderkey = o_orderkey "
            "AND o_orderdate >= 749404800 "
            "AND o_orderdate < 757353600 "
            "AND l_returnflag = 'R' "
            "AND c_nationkey = n_nationkey "
        "GROUP BY "
            "c_custkey, "
            "c_name, "
            "c_acctbal, "
            "c_phone, "
            "n_name, "
            "c_address, "
            "c_comment ";
        // "ORDER BY "
        //     "revenue DESC;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

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

    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    std::vector<std::string> customer_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "customer.tbl_" + std::to_string(i);
        customer_file_names.emplace_back(file_name);
    }
    InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ11) {

	std::string test_sql_tpch = 
        "SELECT "
            "ps_partkey, "
            "SUM(ps_supplycost * ps_availqty) AS value "
        "FROM "
            "partsupp, supplier, nation "
        "WHERE "
            "ps_suppkey = s_suppkey "
            "AND s_nationkey = n_nationkey "
            "AND n_name = 'GERMANY' "
        "GROUP BY "
            "ps_partkey ";
        // "HAVING "
		// 	"SUM(ps_supplycost * ps_availqty) > 1";
        //     "SUM(ps_supplycost * ps_availqty) > ( "
        //         "SELECT "
        //             "SUM(ps_supplycost * ps_availqty) * 0.0001 "
        //         "FROM "
        //             "partsupp, supplier, nation "
        //         "WHERE "
        //             "ps_suppkey = s_suppkey "
        //             "AND s_nationkey = n_nationkey "
        //             "AND n_name = 'GERMANY' "
        //     ") ";
        // "ORDER BY "
        //     "value DESC;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

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

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    std::vector<std::string> partsupp_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "partsupp.tbl_" + std::to_string(i);
        partsupp_file_names.emplace_back(file_name);
    }
    InsertPartsuppMul(table_partsupp, scheduler, work_ids, partition_idxs, partsupp_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ12) {

	std::string test_sql_tpch = 
        "SELECT "
            "l_shipmode, "
            "SUM( "
                // "CASE WHEN "
                //     "o_orderpriority = '1-URGENT' "
                //     "OR o_orderpriority = '2-HIGH' "
                // "THEN 1 "
                // "ELSE 0 "
                // "END) AS high_line_count, "
				"o_orderkey) AS high_line_count, "
            "SUM( "
                // "CASE WHEN "
                //     "o_orderpriority <> '1-URGENT' "
                //     "AND o_orderpriority <> '2-HIGH' "
                // "THEN 1 "
                // "ELSE 0 "
                // "END) AS low_line_count "
				"o_orderkey) AS low_line_count "
        "FROM "
            "orders, lineitem "
        "WHERE "
            "o_orderkey = l_orderkey "
            "AND l_shipmode = 'MAIL' "
            "AND l_commitdate < l_receiptdate "
            "AND l_shipdate < l_commitdate "
            "AND l_receiptdate >= 757353600 "
            "AND l_receiptdate < 788889600 "
        "GROUP BY "
            "l_shipmode "
        "ORDER BY "
            "l_shipmode;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ13) {

	std::string test_sql_tpch = 
        "SELECT c_count, count(*) AS custdist "
        "FROM ( "
            "SELECT c_custkey, count(o_orderkey) AS c_count "
            "FROM CUSTOMER "
            "LEFT OUTER JOIN ORDERS ON c_custkey = o_custkey "
            "WHERE o_comment NOT LIKE '%pending%deposits%' "
            "GROUP BY c_custkey "
        ") c_orders "
        "GROUP BY c_count "
        // "ORDER BY custdist DESC, c_count DESC;";
		"ORDER BY c_count DESC;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    std::vector<std::string> customer_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "customer.tbl_" + std::to_string(i);
        customer_file_names.emplace_back(file_name);
    }
    InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);
    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ14) {

	std::string test_sql_tpch = 
        "SELECT 100.00 * SUM("
		// "CASE WHEN p_type LIKE 'PROMO%' "
		// "THEN l_extendedprice * (1 - l_discount) "
		// "ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue "
		"l_extendedprice * (1 - l_discount) "
		") / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue "
		"FROM lineitem, part "
		"WHERE l_partkey = p_partkey "
		"AND l_shipdate >= 809884800 "
		"AND l_shipdate < 812476800;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ15) {

	std::string test_sql_tpch = 
        "SELECT 100.00 * SUM("
		// "CASE WHEN p_type LIKE 'PROMO%' "
		// "THEN l_extendedprice * (1 - l_discount) "
		// "ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue "
		"l_extendedprice * (1 - l_discount) "
		") / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue "
		"FROM lineitem, part "
		"WHERE l_partkey = p_partkey "
		"AND l_shipdate >= 809884800 "
		"AND l_shipdate < 812476800;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ16) {

	std::string test_sql_tpch = 
        "select "
		"p_brand, "
		"p_type, "
		"p_size, "
		"count(ps_suppkey) as supplier_cnt "
	"from "
		"partsupp, "
		"part "
	"where "
		"p_partkey = ps_partkey "
		"and p_brand != 'Brand#45' "
		"and p_type not like 'MEDIUM POLISHED%' "
		// "and p_size in (49, 14, 23, 45, 19, 3, 36, 9) "
		"and "
		"ps_suppkey in ( "
			"select "
				"s_suppkey "
			"from "
				"supplier "
			"where "
				"s_comment like '%Customer%Complaints%' "
		") "
	// "group by "
	// 	"p_brand, "
	// 	"p_type, "
	// 	"p_size ";
	"order by "
	// // // 	"supplier_cnt desc, "
		"p_brand, "
		"p_type, "
		"p_size; ";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

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

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    std::vector<std::string> supplier_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
        supplier_file_names.emplace_back(file_name);
    }
    InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q6执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ17) {

	std::string test_sql_tpch = 
        "SELECT "
            "SUM(l_extendedprice) AS avg_yearly "
            // "SUM(l_extendedprice) / 7.0 AS avg_yearly "
        "FROM "
            "lineitem, part, "
			"( "
                "SELECT "
                    "AVG(l_quantity) as avg_quantity, "
					"l_partkey "
                "FROM "
                    "lineitem "
				"GROUP BY "
				"l_partkey"
			") "
        "WHERE "
            "p_partkey = l_partkey "
			"AND p_partkey = lineitem.l_partkey "
            "AND p_brand = 'Brand#23' "
            "AND p_container = 'MED BOX' "
            "AND l_quantity < avg_quantity";
            



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q7执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ18) {

	std::string test_sql_tpch = 
		"SELECT "
			"c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, "
			"SUM(l_quantity) "
		"FROM "
			"customer, orders, lineitem "
		"WHERE "
			// "o_orderkey IN ( "
			// 	"SELECT "
			// 		"l_orderkey "
			// 	"FROM "
			// 		"lineitem "
			// 	"GROUP BY "
			// 		"l_orderkey "
			// 	"HAVING "
			// 		"SUM(l_quantity) > 300 "
			// ") "
			" c_custkey = o_custkey "
			"AND o_orderkey = l_orderkey "
		"GROUP BY "
			"c_name, "
			"c_custkey, "
			"o_orderkey, "
			"o_orderdate, "
			"o_totalprice "
		"ORDER BY "
			"o_totalprice DESC, "
			"o_orderdate;";
            



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    std::vector<std::string> customer_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "customer.tbl_" + std::to_string(i);
        customer_file_names.emplace_back(file_name);
    }
    InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q8执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ19) {

	std::string test_sql_tpch = 
		"SELECT "
            "SUM(l_extendedprice * (1 - l_discount)) AS revenue "
        "FROM "
            "lineitem, part "
        "WHERE ("
            "p_partkey = l_partkey "
            "AND p_brand = 'Brand#12' "
            // "AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
            "AND l_quantity >= 1 AND l_quantity <= 11 "
            "AND p_size >= 1 AND p_size <= 5 "
            // "AND l_shipmode IN ('AIR', 'AIR REG') "
            "AND l_shipinstruct = 'DELIVER IN PERSON' "
        // ") OR ("
		") AND ("
            "p_partkey = l_partkey "
            "AND p_brand = 'Brand#23' "
            // "AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
            "AND l_quantity >= 10 AND l_quantity <= 20 "
            "AND p_size >= 1 AND p_size <= 10 "
            // "AND l_shipmode IN ('AIR', 'AIR REG') "
            "AND l_shipinstruct = 'DELIVER IN PERSON' "
        // ") OR ("
		") AND ("
            "p_partkey = l_partkey "
            "AND p_brand = 'Brand#34' "
            // "AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
            "AND l_quantity >= 20 AND l_quantity <= 30 "
            "AND p_size >= 1 AND p_size <= 15 "
            // "AND l_shipmode IN ('AIR', 'AIR REG') "
            "AND l_shipinstruct = 'DELIVER IN PERSON' "
        ");";
            



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

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

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q9执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ20) {

	std::string test_sql_tpch = 
		"SELECT "
            "s_name, s_address "
        "FROM "
            "supplier, nation "
        "WHERE "
            // "s_suppkey IN ( "
			"s_suppkey = ( "
                "SELECT "
                    "MIN(ps_suppkey) "
                "FROM "
                    "partsupp "
                // "WHERE "
                //     // "ps_partkey IN ( "
				// 	"ps_partkey = ( "
                //         "SELECT "
                //             "p_partkey "
                //         "FROM "
                //             "part "
                //         "WHERE "
                //             "p_name LIKE 'forest%' "
                //     ") "
                    // "AND ps_availqty > ( "
                    //     "SELECT "
                    //         "0.5 * SUM(l_quantity) "
                    //     "FROM "
                    //         "lineitem "
                    //     "WHERE "
                    //         "l_partkey = ps_partkey "
                    //         "AND l_suppkey = ps_suppkey "
                    //         "AND l_shipdate >= DATE '1994-01-01' "
                    //         "AND l_shipdate < DATE '1995-01-01' "
                    // ") "
            ") "
            "AND s_nationkey = n_nationkey "
            "AND n_name = 'CANADA' ";
        // "ORDER BY "
        //     "s_name;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

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

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ21) {

	std::string test_sql_tpch = 
		"SELECT "
            "s_name, COUNT(*) AS numwait "
        "FROM "
            "supplier, lineitem l1, orders, nation "
        "WHERE "
            "s_suppkey = l1.l_suppkey "
            "AND o_orderkey = l1.l_orderkey "
            "AND o_orderstatus = 'F' "
            "AND l1.l_receiptdate > l1.l_commitdate "
            "AND EXISTS ( "
                "SELECT "
                    "* "
                "FROM "
                    "lineitem l2 "
                "WHERE "
                    "l2.l_orderkey = l1.l_orderkey "
                    "AND l2.l_suppkey <> l1.l_suppkey "
            ") "
            "AND NOT EXISTS ( "
                "SELECT "
                    "* "
                "FROM "
                    "lineitem l3 "
                "WHERE "
                    "l3.l_orderkey = l1.l_orderkey "
                    "AND l3.l_suppkey <> l1.l_suppkey "
                    "AND l3.l_receiptdate > l3.l_commitdate "
            ") "
            "AND s_nationkey = n_nationkey "
            "AND n_name = 'SAUDI ARABIA' "
        "GROUP BY "
            "s_name "
        "ORDER BY "
            "numwait DESC, "
            "s_name;";

    // SELECT 
    //     s.s_name,
    //     COUNT(*) AS numwait
    // FROM 
    //     supplier s,
    //     lineitem l1,
    //     orders o,
    //     nation n,
    //     (
    //         SELECT l2.l_orderkey
    //         FROM lineitem l2
    //         GROUP BY l2.l_orderkey
    //         HAVING COUNT(DISTINCT l2.l_suppkey) > 1
    //     ) subquery_l2,
    //     (
    //         SELECT l3.l_orderkey
    //         FROM lineitem l3
    //         WHERE l3.l_receiptdate > l3.l_commitdate
    //         GROUP BY l3.l_orderkey
    //         HAVING COUNT(DISTINCT l3.l_suppkey) > 1
    //     ) subquery_l3
    // WHERE 
    //     s.s_suppkey = l1.l_suppkey
    //     AND o.o_orderkey = l1.l_orderkey
    //     AND s.s_nationkey = n.n_nationkey
    //     AND l1.l_orderkey = subquery_l2.l_orderkey
    //     AND (l1.l_orderkey = subquery_l3.l_orderkey OR subquery_l3.l_orderkey IS NULL)
    //     AND o.o_orderstatus = 'F'
    //     AND l1.l_receiptdate > l1.l_commitdate
    //     AND n.n_name = 'SAUDI ARABIA'
    //     AND subquery_l3.l_orderkey IS NULL
    // GROUP BY 
    //     s.s_name
    // ORDER BY 
    //     numwait DESC,
    //     s.s_name;


	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

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
    std::this_thread::sleep_for(std::chrono::seconds(2));
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

TEST(TPCHBySqlsTest, TPCHBySqlsTestQ22) {

	std::string test_sql_tpch = 
		"SELECT "
            // "cntrycode, "
            "COUNT(*) AS numcust, "
            "SUM(c_acctbal) AS totacctbal "
        "FROM ( "
            "SELECT "
                // "SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, "
                "c_acctbal "
            "FROM "
                "customer "
            "WHERE "
                // "SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '30', '18', '17') "
                "c_acctbal > ( "
                    "SELECT "
                        "AVG(c_acctbal) "
                    "FROM "
                        "customer "
                    "WHERE "
                        "c_acctbal > 0.00 "
                        // "AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '30', '18', '17') "
                ") "
                // "AND NOT EXISTS ( "
                //     "SELECT "
                //         "* "
                //     "FROM "
                //         "orders "
                //     "WHERE "
                //         "o_custkey = c_custkey "
                // ") "
        ") AS custsale ";
        // "GROUP BY "
        //     "cntrycode "
        // "ORDER BY "
        //     "cntrycode;";



	std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
	bool importFinish = false;

    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    std::vector<std::string> customer_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "customer.tbl_" + std::to_string(i);
        customer_file_names.emplace_back(file_name);
    }
    InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;

	scheduler->shutdown();
	/////

	QueryHandler query_handler(global_catalog);
	
	auto start_execute = std::chrono::steady_clock::now();
	RC rc = query_handler.Query(test_sql_tpch);

	ASSERT_EQ(RC::SUCCESS, rc);

	auto end_execute = std::chrono::steady_clock::now();

	spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}
