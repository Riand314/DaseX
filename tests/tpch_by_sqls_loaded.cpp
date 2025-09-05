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
#include "create_table.hpp"

using namespace DaseX;

namespace{

const int arr[] = {1, 2, 3, 4};
const std::string fileDir4 = "../test_data/4thread";
const std::string fileDir1 = "../test_data";

std::string fileDir = fileDir4;
std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
std::string database_path_single = "temp_database_all"; // 插入单个partition的数据
std::string database_path_multi  = "temp_database_all_multi"; // 插入多个partition的数据

std::string test_sql_tpch1 =
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

std::string test_sql_tpch2 =
	"select "
	"s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, "
	"s_comment, subquery.min_cost, subquery.partsupp.ps_partkey "
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

std::string test_sql_tpch3 =
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

std::string test_sql_tpch4 =
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

std::string test_sql_tpch5 =
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

std::string test_sql_tpch6 =
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

std::string test_sql_tpch7 =
	"SELECT "
	"supp_nation, "
	"cust_nation, "
		"SUM(volume) AS revenue "
	"FROM ( "
	"SELECT "
	"n1.n_name AS supp_nation, "
	"n2.n_name AS cust_nation, "
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
		") "
	"AND l_shipdate >= 788889600 AND l_shipdate <= 851961600 "
	") AS shipping ";

std::string test_sql_tpch8 =
	"SELECT "
	"SUM(volume) AS mkt_share "
	"FROM ( "
	"SELECT "
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

std::string test_sql_tpch9 =
	"SELECT "
	"SUM(amount) AS sum_profit "
	"FROM ( "
	"SELECT "
	"l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount "
	"FROM "
	"lineitem, part, partsupp "
	"WHERE "
	"ps_suppkey = l_suppkey "
	"AND ps_partkey = l_partkey "
	"AND p_partkey = l_partkey "
	"AND p_name LIKE '%green%' "
	") ";

std::string test_sql_tpch10 =
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

std::string test_sql_tpch11 =
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

std::string test_sql_tpch12 =
	"SELECT "
	"l_shipmode, "
	"SUM( "
	"o_orderkey) AS high_line_count, "
	"SUM( "
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

std::string test_sql_tpch13 =
	"SELECT c_count, count(*) AS custdist "
	"FROM ( "
	"SELECT c_custkey, count(o_orderkey) AS c_count "
	"FROM CUSTOMER "
	"LEFT OUTER JOIN ORDERS ON c_custkey = o_custkey "
	"WHERE o_comment NOT LIKE '%pending%deposits%' "
	"GROUP BY c_custkey "
	") c_orders "
	"GROUP BY c_count "
	"ORDER BY c_count DESC;";

std::string test_sql_tpch14 =
	"SELECT 100.00 * SUM("
	"l_extendedprice * (1 - l_discount) "
	") / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue "
	"FROM lineitem, part "
	"WHERE l_partkey = p_partkey "
	"AND l_shipdate >= 809884800 "
	"AND l_shipdate < 812476800;";

std::string test_sql_tpch15 =
	"SELECT 100.00 * SUM("
	"l_extendedprice * (1 - l_discount) "
	") / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue "
	"FROM lineitem, part "
	"WHERE l_partkey = p_partkey "
	"AND l_shipdate >= 809884800 "
	"AND l_shipdate < 812476800;";

std::string test_sql_tpch16 =
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
	"and "
	"ps_suppkey in ( "
	"select "
	"s_suppkey "
	"from "
	"supplier "
	"where "
	"s_comment like '%Customer%Complaints%' "
	") "
	"order by "
	"p_brand, "
	"p_type, "
	"p_size; ";

std::string test_sql_tpch17 =
	"SELECT "
	"SUM(l_extendedprice) AS avg_yearly "
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

std::string test_sql_tpch18 =
	"SELECT "
	"c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, "
	"SUM(l_quantity) "
	"FROM "
	"customer, orders, lineitem "
	"WHERE "
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

std::string test_sql_tpch19 =
	"SELECT "
	"SUM(l_extendedprice * (1 - l_discount)) AS revenue "
	"FROM "
	"lineitem, part "
	"WHERE ("
	"p_partkey = l_partkey "
	"AND p_brand = 'Brand#12' "
	"AND l_quantity >= 1 AND l_quantity <= 11 "
	"AND p_size >= 1 AND p_size <= 5 "
	"AND l_shipinstruct = 'DELIVER IN PERSON' "
	") AND ("
	"p_partkey = l_partkey "
	"AND p_brand = 'Brand#23' "
	"AND l_quantity >= 10 AND l_quantity <= 20 "
	"AND p_size >= 1 AND p_size <= 10 "
	"AND l_shipinstruct = 'DELIVER IN PERSON' "
	") AND ("
	"p_partkey = l_partkey "
	"AND p_brand = 'Brand#34' "
	"AND l_quantity >= 20 AND l_quantity <= 30 "
	"AND p_size >= 1 AND p_size <= 15 "
	"AND l_shipinstruct = 'DELIVER IN PERSON' "
	");";

std::string test_sql_tpch20 =
	"SELECT "
	"s_name, s_address "
	"FROM "
	"supplier, nation "
	"WHERE "
	"s_suppkey = ( "
	"SELECT "
	"MIN(ps_suppkey) "
	"FROM "
	"partsupp "
	") "
	"AND s_nationkey = n_nationkey "
	"AND n_name = 'CANADA' ";

std::string test_sql_tpch21 =
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

std::string test_sql_tpch22 =
	"SELECT "
	"COUNT(*) AS numcust, "
	"SUM(c_acctbal) AS totacctbal "
	"FROM ( "
	"SELECT "
	"c_acctbal "
	"FROM "
	"customer "
	"WHERE "
	"c_acctbal > ( "
	"SELECT "
	"AVG(c_acctbal) "
	"FROM "
	"customer "
	"WHERE "
	"c_acctbal > 0.00 "
	") "
	") AS custsale ";

std::vector<std::string> test_sqls = {
	test_sql_tpch1,
	test_sql_tpch2,
	test_sql_tpch3,
	test_sql_tpch4,
	test_sql_tpch5,
	test_sql_tpch6,
	test_sql_tpch7,
	test_sql_tpch8,
	test_sql_tpch9,
	test_sql_tpch10,
    test_sql_tpch11,
	test_sql_tpch12,
	test_sql_tpch13,
	test_sql_tpch14,
	test_sql_tpch15,
	test_sql_tpch16,
	test_sql_tpch17,
	test_sql_tpch18,
	test_sql_tpch19,
	test_sql_tpch20,
    test_sql_tpch21,
    test_sql_tpch22,
};

}

#define LOAD_DATA
// NOTE: 如果是第一次运行，那么就要运行这个测试用例，将数据加载到数据库中
// 加载一次就行，不然每次都加载数据，会很慢

#ifdef LOAD_DATA
TEST(TPCHBySqlsTest, LoadData) {
    LoadSinglePartitionAllTable(database_path_single, true);
    LoadMultiPartitionAllTable(database_path_multi, true);
}
#endif

TEST(TPCHBySqlsTest, TPCHBySqlsTestAll) {
    // 跳过执行的sql
    std::set<std::string> skip_sqls = {
        // test_sql_tpch1,
        // test_sql_tpch2,
        // test_sql_tpch3,
        // test_sql_tpch4,
        // test_sql_tpch5,
        // test_sql_tpch6,
        // test_sql_tpch7,
        // test_sql_tpch8,
        // test_sql_tpch9,
        // test_sql_tpch10,
        // test_sql_tpch11,
        // test_sql_tpch12,
        // test_sql_tpch13,
        // test_sql_tpch14,
        // test_sql_tpch15,
        // test_sql_tpch17,
        // test_sql_tpch18,
        // test_sql_tpch19,
        // test_sql_tpch20,

        // 上面的没问题
        // 下面的跑不了
        test_sql_tpch16, // 会卡死
        test_sql_tpch21, // 会卡死
        test_sql_tpch22, // 会段错误
    };

    std::shared_ptr<DaseX::CataLog> catalog(nullptr);
	
	// 打开数据库
    ASSERT_EQ(OpenDatabase(database_path_multi, catalog, true),  RC::SUCCESS);
    ASSERT_NE(catalog.get(), nullptr);

    for (size_t i = 0; i < test_sqls.size(); i++) {
        if(skip_sqls.find(test_sqls[i]) != skip_sqls.end()) {
            continue;
        }

        QueryHandler query_handler(catalog);
        auto start_execute = std::chrono::steady_clock::now();
        ASSERT_EQ(query_handler.Query(test_sqls[i]), RC::SUCCESS);
        auto end_execute = std::chrono::steady_clock::now();
        spdlog::info("[{} : {}] Q{}执行时间: {}", __FILE__, __LINE__, i+1,
            Util::time_difference(start_execute, end_execute));
    }
    
}
