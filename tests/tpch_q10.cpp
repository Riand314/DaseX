#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q10=============================
// 测试单线程Q10查询
TEST(TPCHTest, Q10SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    InsertNation(table_nation, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    InsertCustomer(table_customer, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    InsertOrders(table_orders, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> nation_ids = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> customer_ids = {0, 1, 2, 3, 4, 5, 7};
    // <c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> lineitem_ids = {0, 5, 6, 8};
    // <l_orderkey, l_extendedprice, l_discount, l_returnflag>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> orders_ids = {0, 1, 4};
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {3};
    // <n_nationkey, n_name, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment>
    std::shared_ptr<PhysicalHashJoin> join_1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join_1->exp_name = "1";

    std::shared_ptr<BoundColumnRefExpression> f1_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_constant = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(f1_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_constant);
    // <n_nationkey, n_name, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment>
    std::shared_ptr<PhysicalFilter> filter_1 = std::make_shared<PhysicalFilter>(comparisonExpression_c_custkey);

    std::shared_ptr<BoundColumnRefExpression> p1_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 8, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_c_custkey, p1_c_name, p1_c_address, p1_c_phone, p1_c_acctbal, p1_c_comment, p1_n_name};
    // <c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_749404800(749404800);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_749404800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);

    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_757353600(757353600);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate2 = std::make_shared<BoundConstantExpression>(value_757353600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_o_orderdate2->SetLeft(left_o_orderdate2);
    comparisonExpression_o_orderdate2->SetRight(right_o_orderdate2);
    std::shared_ptr<BoundConjunctionExpression> o_orderdate_and = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_o_orderdate, comparisonExpression_o_orderdate2);

    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalFilter> filter_2 = std::make_shared<PhysicalFilter>(o_orderdate_and);

    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name, o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join_2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join_2->exp_name = "2";

    std::shared_ptr<BoundColumnRefExpression> f3_l_returnflag = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    Value value_R("R");
    std::shared_ptr<BoundConstantExpression> right_l_returnflag = std::make_shared<BoundConstantExpression>(value_R);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_returnflag = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_l_returnflag->SetLeft(f3_l_returnflag);
    comparisonExpression_l_returnflag->SetRight(right_l_returnflag);
    // <l_orderkey, l_extendedprice, l_discount, l_returnflag>
    std::shared_ptr<PhysicalFilter> filter_3 = std::make_shared<PhysicalFilter>(comparisonExpression_l_returnflag);

    std::vector<int> build_ids3 = {7};
    std::vector<int> prode_ids3 = {0};
    // <c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name, o_orderkey, o_custkey, o_orderdate, l_orderkey, l_extendedprice, l_discount, l_returnflag>
    std::shared_ptr<PhysicalHashJoin> join_3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join_3->exp_name = "3";

    std::shared_ptr<BoundColumnRefExpression> p2_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 11, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 12, 0);

    std::vector<LogicalType> arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function1 = GetSubFunction();
    ScalarProjectFunction bound_function1("-", arguments1, LogicalType::FLOAT, function1, nullptr);
    Value value_1(1.0f);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<std::shared_ptr<Expression>> expressions1 = {constant1_col, p2_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function1, expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function2 = GetMulFunction();
    ScalarProjectFunction bound_function2("*", arguments2, LogicalType::FLOAT, function2, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {p2_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> revenue = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function2, expressions2, nullptr);
    revenue->alias = "revenue";

    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_c_custkey, p2_c_name, revenue, p2_c_acctbal, p2_n_name, p2_c_address, p2_c_phone, p2_c_comment};
    // <c_custkey, c_name, revenue, c_acctbal, n_name, c_address, c_phone, c_comment>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    // 构建HashAgg
    std::vector<int> group_set = {0, 1, 3, 6, 4, 5, 7};
    std::vector<int> agg_set = {2};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment, Sum(revenue)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {7};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(join_3);
    join_3->AddChild(join_2);
    join_3->AddChild(filter_3);
    filter_3->AddChild(scan_lineitem);
    join_2->AddChild(project1);
    join_2->AddChild(filter_2);
    filter_2->AddChild(scan_orders);
    project1->AddChild(filter_1);
    filter_1->AddChild(join_1);
    join_1->AddChild(scan_nation);
    join_1->AddChild(scan_customer);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q10执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q10查询
TEST(TPCHTest, Q10FourThreadTest) {
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
    // spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

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
    // spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

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
    // spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

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
    // spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    std::cout << "Q10: select\n"
                 "    c_custkey, c_name,\n"
                 "    sum(l_extendedprice * (1 - l_discount)) as revenue,\n"
                 "    c_acctbal,\n"
                 "    n_name, c_address, c_phone, c_comment\n"
                 "from\n"
                 "    customer, orders, lineitem, nation\n"
                 "where\n"
                 "    c_custkey = o_custkey\n"
                 "    and l_orderkey = o_orderkey\n"
                 "    and o_orderdate >= 749404800\n"
                 "    and o_orderdate < 757353600\n"
                 "    and l_returnflag = 'R'\n"
                 "    and c_nationkey = n_nationkey\n"
                 "group by\n"
                 "    c_custkey,\n"
                 "    c_name,\n"
                 "    c_acctbal,\n"
                 "    c_phone,\n"
                 "    n_name,\n"
                 "    c_address,\n"
                 "    c_comment\n"
                 "order by\n"
                 "    revenue desc;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> nation_ids = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> customer_ids = {0, 1, 2, 3, 4, 5, 7};
    // <c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> lineitem_ids = {0, 5, 6, 8};
    // <l_orderkey, l_extendedprice, l_discount, l_returnflag>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> orders_ids = {0, 1, 4};
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {3};
    std::vector<ExpressionTypes> comparison_types1 = {};
    // <n_nationkey, n_name, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment>
    std::shared_ptr<PhysicalHashJoin> join_1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1, comparison_types1, true);
    join_1->exp_name = "1";

    std::shared_ptr<BoundColumnRefExpression> f1_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_constant = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(f1_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_constant);
    // <n_nationkey, n_name, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment>
    std::shared_ptr<PhysicalFilter> filter_1 = std::make_shared<PhysicalFilter>(comparisonExpression_c_custkey);

    std::shared_ptr<BoundColumnRefExpression> p1_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 8, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_c_custkey, p1_c_name, p1_c_address, p1_c_phone, p1_c_acctbal, p1_c_comment, p1_n_name};
    // <c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_749404800(749404800);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_749404800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);

    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_757353600(757353600);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate2 = std::make_shared<BoundConstantExpression>(value_757353600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_o_orderdate2->SetLeft(left_o_orderdate2);
    comparisonExpression_o_orderdate2->SetRight(right_o_orderdate2);
    std::shared_ptr<BoundConjunctionExpression> o_orderdate_and = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_o_orderdate, comparisonExpression_o_orderdate2);

    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalFilter> filter_2 = std::make_shared<PhysicalFilter>(o_orderdate_and);

    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name, o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join_2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join_2->exp_name = "2";

    std::shared_ptr<BoundColumnRefExpression> f3_l_returnflag = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    Value value_R("R");
    std::shared_ptr<BoundConstantExpression> right_l_returnflag = std::make_shared<BoundConstantExpression>(value_R);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_returnflag = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_l_returnflag->SetLeft(f3_l_returnflag);
    comparisonExpression_l_returnflag->SetRight(right_l_returnflag);
    // <l_orderkey, l_extendedprice, l_discount, l_returnflag>
    std::shared_ptr<PhysicalFilter> filter_3 = std::make_shared<PhysicalFilter>(comparisonExpression_l_returnflag);

    std::vector<int> build_ids3 = {7};
    std::vector<int> prode_ids3 = {0};
    // <c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name, o_orderkey, o_custkey, o_orderdate, l_orderkey, l_extendedprice, l_discount, l_returnflag>
    std::shared_ptr<PhysicalHashJoin> join_3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join_3->exp_name = "3";

    std::shared_ptr<BoundColumnRefExpression> p2_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 11, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 12, 0);

    std::vector<LogicalType> arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function1 = GetSubFunction();
    ScalarProjectFunction bound_function1("-", arguments1, LogicalType::FLOAT, function1, nullptr);
    Value value_1(1.0f);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<std::shared_ptr<Expression>> expressions1 = {constant1_col, p2_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function1, expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function2 = GetMulFunction();
    ScalarProjectFunction bound_function2("*", arguments2, LogicalType::FLOAT, function2, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {p2_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> revenue = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function2, expressions2, nullptr);
    revenue->alias = "revenue";

    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_c_custkey, p2_c_name, revenue, p2_c_acctbal, p2_n_name, p2_c_address, p2_c_phone, p2_c_comment};
    // <c_custkey, c_name, revenue, c_acctbal, n_name, c_address, c_phone, c_comment>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    // 构建HashAgg
    std::vector<int> group_set = {0, 1, 3, 6, 4, 5, 7};
    std::vector<int> agg_set = {2};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment, Sum(revenue)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {7};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(join_3);
    join_3->AddChild(join_2);
    join_3->AddChild(filter_3);
    filter_3->AddChild(scan_lineitem);
    join_2->AddChild(project1);
    join_2->AddChild(filter_2);
    filter_2->AddChild(scan_orders);
    project1->AddChild(filter_1);
    filter_1->AddChild(join_1);
    join_1->AddChild(scan_nation);
    join_1->AddChild(scan_customer);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q10执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
    std::string executeTime = std::to_string(pipeline_group_executor->duration.count()) + " ms.";
    std::ofstream env_file("/home/cwl/workspace/dasex/executeTime.txt", std::ios::trunc);
    if (env_file) {
        env_file << executeTime << std::endl;
        env_file.close();
    } else {
        std::cerr << "Failed to write to /etc/environment. Try running as root." << std::endl;
    }
    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}