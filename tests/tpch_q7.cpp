#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q7=============================
// 测试单线程Q7查询
TEST(TPCHTest, Q7SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    InsertSupplier(table_supplier, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    InsertOrders(table_orders, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    InsertCustomer(table_customer, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    InsertNation(table_nation, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> nation_ids = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation2 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> customer_ids = {0, 3};
    // <c_custkey, c_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> orders_ids = {0, 1};
    // <o_orderkey, o_custkey>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> lineitem_ids = {0, 2, 5, 6, 10};
    // <l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {1};
    std::vector<ExpressionTypes> comparison_types1 = {};
    // <n_nationkey, n_name, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1, comparison_types1, true);
    join1->exp_name = "1";
    //  ================================================构建Filter-customer================================================
    std::shared_ptr<BoundColumnRefExpression> left_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(left_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_c_custkey);
    // <n_nationkey, n_name, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_c_custkey);

    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_c_custkey, p1_n_name};
    // <c_custkey, n_name>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <c_custkey, n_name, o_orderkey, o_custkey>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::vector<int> build_ids3 = {2};
    std::vector<int> prode_ids3 = {0};
    // <c_custkey, n_name, o_orderkey, o_custkey, l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "3";

    std::vector<int> build_ids4 = {0};
    std::vector<int> prode_ids4 = {1};
    std::vector<ExpressionTypes> comparison_types4 = {};
    // <n_nationkey, n_name, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4, comparison_types4, true);
    join4->exp_name = "4";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids5 = {2};
    std::vector<int> prode_ids5 = {5};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, c_custkey, n_name, o_orderkey, o_custkey, l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids5, prode_ids5);
    join5->exp_name = "5";
    //  ================================================构建Filter-lineitem================================================
    std::shared_ptr<BoundColumnRefExpression> left_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    Value value_788889600(788889600);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate = std::make_shared<BoundConstantExpression>(value_788889600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_l_shipdate->SetLeft(left_l_shipdate);
    comparisonExpression_l_shipdate->SetRight(right_l_shipdate);

    std::shared_ptr<BoundColumnRefExpression> left_l_shipdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    Value value_851961600(851961600);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate2 = std::make_shared<BoundConstantExpression>(value_851961600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_l_shipdate2->SetLeft(left_l_shipdate2);
    comparisonExpression_l_shipdate2->SetRight(right_l_shipdate2);

    std::shared_ptr<BoundConjunctionExpression> l_shipdate_and = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_shipdate, comparisonExpression_l_shipdate2);

    // <l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(l_shipdate_and);

    //  ================================================构建Filter-join================================================
    std::shared_ptr<BoundColumnRefExpression> left_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    Value value_FRANCE("FRANCE");
    std::shared_ptr<BoundConstantExpression> right_n_name = std::make_shared<BoundConstantExpression>(value_FRANCE);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name->SetLeft(left_n_name);
    comparisonExpression_n_name->SetRight(right_n_name);

    std::shared_ptr<BoundColumnRefExpression> left_n_name2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_GERMANY("GERMANY");
    std::shared_ptr<BoundConstantExpression> right_n_name2 = std::make_shared<BoundConstantExpression>(value_GERMANY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name2->SetLeft(left_n_name2);
    comparisonExpression_n_name2->SetRight(right_n_name2);

    std::shared_ptr<BoundConjunctionExpression> n_name_and = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_n_name, comparisonExpression_n_name2);

    std::shared_ptr<BoundColumnRefExpression> left_n_name3 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundConstantExpression> right_n_name3 = std::make_shared<BoundConstantExpression>(value_GERMANY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name3->SetLeft(left_n_name3);
    comparisonExpression_n_name3->SetRight(right_n_name3);

    std::shared_ptr<BoundColumnRefExpression> left_n_name4 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundConstantExpression> right_n_name4 = std::make_shared<BoundConstantExpression>(value_FRANCE);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name4 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name4->SetLeft(left_n_name4);
    comparisonExpression_n_name4->SetRight(right_n_name4);

    std::shared_ptr<BoundConjunctionExpression> n_name_and2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_n_name3, comparisonExpression_n_name4);

    std::shared_ptr<BoundConjunctionExpression> n_name_or = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, n_name_and, n_name_and2);

    // <n_nationkey, n_name, s_suppkey, s_nationkey,
    // c_custkey, n_name, o_orderkey, o_custkey, l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(n_name_or);

    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p_n1_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> p_n2_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 12, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 10, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 11, 0);

    std::vector<LogicalType> arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function1 = GetSubFunction();
    ScalarProjectFunction bound_function1("-", arguments1, LogicalType::FLOAT, function1, nullptr);
    Value value_1(1.0f);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<std::shared_ptr<Expression>> expressions1 = {constant1_col, p_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function1, expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function2 = GetMulFunction();
    ScalarProjectFunction bound_function2("*", arguments2, LogicalType::FLOAT, function2, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {p_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function2, expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice*(1-l_discount)";

    std::vector<LogicalType> arguments3 = { LogicalType::INTEGER, LogicalType::INTEGER };
    scalar_function_p function3 = GetExtractYearFunction();
    ScalarProjectFunction bound_function3("extract_year", arguments3, LogicalType::INTEGER, function3, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions3 = {p_l_shipdate, p_l_shipdate};
    std::shared_ptr<BoundProjectFunctionExpression> extract_year = std::make_shared<BoundProjectFunctionExpression>(LogicalType::INTEGER, bound_function3, expressions3, nullptr);
    extract_year->alias = "l_year";

    std::vector<std::shared_ptr<Expression>> p_expressions = {p_n1_name, p_n2_name, extract_year, extendedprice_discount_1_col};
    // <n_name, n_name, l_year, l_extendedprice * (1 - l_discount)>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p_expressions);

    // 构建HashAgg
    std::vector<int> group_set = {0, 1, 2};
    std::vector<int> agg_set = {3};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <n_name, n_name, l_year, Sum(l_extendedprice * (1 - l_discount))>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0, 1, 2};
    std::vector<LogicalType> key_types = {LogicalType::STRING, LogicalType::STRING, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(filter3);
    filter3->AddChild(join5);
    join5->AddChild(join4);
    join5->AddChild(join3);
    join3->AddChild(join2);
    join3->AddChild(filter2);
    join2->AddChild(project1);
    join2->AddChild(scan_orders);
    project1->AddChild(join1);
    // filter1->AddChild(join1);
    join1->AddChild(scan_nation);
    join1->AddChild(scan_customer);
    filter2->AddChild(scan_lineitem);
    join4->AddChild(scan_nation2);
    join4->AddChild(scan_supplier);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q7执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q7查询
TEST(TPCHTest, Q7FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
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
    // spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

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
    // spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

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
    // spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    std::cout << "Q7: select\n"
                 "    supp_nation,\n"
                 "    cust_nation,\n"
                 "    l_year, sum(volume) as revenue\n"
                 "from (\n"
                 "  select\n"
                 "  n1.n_name as supp_nation,\n"
                 "  n2.n_name as cust_nation,\n"
                 "  extract(year from l_shipdate) as l_year,\n"
                 "  l_extendedprice * (1 - l_discount) as volume\n"
                 "  from\n"
                 "  supplier,lineitem,orders,customer,nation n1,nation n2\n"
                 "  where\n"
                 "  s_suppkey = l_suppkey\n"
                 "  and o_orderkey = l_orderkey\n"
                 "  and c_custkey = o_custkey\n"
                 "  and s_nationkey = n1.n_nationkey\n"
                 "  and c_nationkey = n2.n_nationkey\n"
                 "  and (\n"
                 "    (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')\n"
                 "    or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')\n"
                 "  )\n"
                 "  and l_shipdate >= 788889600 and l_shipdate <= 851961600\n"
                 ") as shipping\n"
                 "group by\n"
                 "    supp_nation,\n"
                 "    cust_nation,\n"
                 "    l_year\n"
                 "order by\n"
                 "    supp_nation,\n"
                 "    cust_nation,\n"
                 "    l_year;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> nation_ids = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation2 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> customer_ids = {0, 3};
    // <c_custkey, c_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> orders_ids = {0, 1};
    // <o_orderkey, o_custkey>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> lineitem_ids = {0, 2, 5, 6, 10};
    // <l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {1};
    std::vector<ExpressionTypes> comparison_types1 = {};
    // <n_nationkey, n_name, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1, comparison_types1, true);
    join1->exp_name = "1";
    //  ================================================构建Filter-customer================================================
    std::shared_ptr<BoundColumnRefExpression> left_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(left_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_c_custkey);
    // <n_nationkey, n_name, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_c_custkey);

    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_c_custkey, p1_n_name};
    // <c_custkey, n_name>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <c_custkey, n_name, o_orderkey, o_custkey>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::vector<int> build_ids3 = {2};
    std::vector<int> prode_ids3 = {0};
    // <c_custkey, n_name, o_orderkey, o_custkey, l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "3";

    std::vector<int> build_ids4 = {0};
    std::vector<int> prode_ids4 = {1};
    std::vector<ExpressionTypes> comparison_types4 = {};
    // <n_nationkey, n_name, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4, comparison_types4, true);
    join4->exp_name = "4";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids5 = {2};
    std::vector<int> prode_ids5 = {5};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, c_custkey, n_name, o_orderkey, o_custkey, l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids5, prode_ids5);
    join5->exp_name = "5";
    //  ================================================构建Filter-lineitem================================================
    std::shared_ptr<BoundColumnRefExpression> left_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    Value value_788889600(788889600);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate = std::make_shared<BoundConstantExpression>(value_788889600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_l_shipdate->SetLeft(left_l_shipdate);
    comparisonExpression_l_shipdate->SetRight(right_l_shipdate);

    std::shared_ptr<BoundColumnRefExpression> left_l_shipdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    Value value_851961600(851961600);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate2 = std::make_shared<BoundConstantExpression>(value_851961600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_l_shipdate2->SetLeft(left_l_shipdate2);
    comparisonExpression_l_shipdate2->SetRight(right_l_shipdate2);

    std::shared_ptr<BoundConjunctionExpression> l_shipdate_and = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_shipdate, comparisonExpression_l_shipdate2);

    // <l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(l_shipdate_and);

    //  ================================================构建Filter-join================================================
    std::shared_ptr<BoundColumnRefExpression> left_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    Value value_FRANCE("FRANCE");
    std::shared_ptr<BoundConstantExpression> right_n_name = std::make_shared<BoundConstantExpression>(value_FRANCE);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name->SetLeft(left_n_name);
    comparisonExpression_n_name->SetRight(right_n_name);

    std::shared_ptr<BoundColumnRefExpression> left_n_name2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_GERMANY("GERMANY");
    std::shared_ptr<BoundConstantExpression> right_n_name2 = std::make_shared<BoundConstantExpression>(value_GERMANY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name2->SetLeft(left_n_name2);
    comparisonExpression_n_name2->SetRight(right_n_name2);

    std::shared_ptr<BoundConjunctionExpression> n_name_and = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_n_name, comparisonExpression_n_name2);

    std::shared_ptr<BoundColumnRefExpression> left_n_name3 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundConstantExpression> right_n_name3 = std::make_shared<BoundConstantExpression>(value_GERMANY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name3->SetLeft(left_n_name3);
    comparisonExpression_n_name3->SetRight(right_n_name3);

    std::shared_ptr<BoundColumnRefExpression> left_n_name4 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundConstantExpression> right_n_name4 = std::make_shared<BoundConstantExpression>(value_FRANCE);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name4 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name4->SetLeft(left_n_name4);
    comparisonExpression_n_name4->SetRight(right_n_name4);

    std::shared_ptr<BoundConjunctionExpression> n_name_and2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_n_name3, comparisonExpression_n_name4);

    std::shared_ptr<BoundConjunctionExpression> n_name_or = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, n_name_and, n_name_and2);

    // <n_nationkey, n_name, s_suppkey, s_nationkey,
    // c_custkey, n_name, o_orderkey, o_custkey, l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(n_name_or);

    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p_n1_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> p_n2_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 12, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 10, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 11, 0);

    std::vector<LogicalType> arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function1 = GetSubFunction();
    ScalarProjectFunction bound_function1("-", arguments1, LogicalType::FLOAT, function1, nullptr);
    Value value_1(1.0f);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<std::shared_ptr<Expression>> expressions1 = {constant1_col, p_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function1, expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function2 = GetMulFunction();
    ScalarProjectFunction bound_function2("*", arguments2, LogicalType::FLOAT, function2, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {p_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function2, expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice*(1-l_discount)";

    std::vector<LogicalType> arguments3 = { LogicalType::INTEGER, LogicalType::INTEGER };
    scalar_function_p function3 = GetExtractYearFunction();
    ScalarProjectFunction bound_function3("extract_year", arguments3, LogicalType::INTEGER, function3, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions3 = {p_l_shipdate, p_l_shipdate};
    std::shared_ptr<BoundProjectFunctionExpression> extract_year = std::make_shared<BoundProjectFunctionExpression>(LogicalType::INTEGER, bound_function3, expressions3, nullptr);
    extract_year->alias = "l_year";

    std::vector<std::shared_ptr<Expression>> p_expressions = {p_n1_name, p_n2_name, extract_year, extendedprice_discount_1_col};
    // <n_name, n_name, l_year, l_extendedprice * (1 - l_discount)>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p_expressions);

    // 构建HashAgg
    std::vector<int> group_set = {0, 1, 2};
    std::vector<int> agg_set = {3};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <n_name, n_name, l_year, Sum(l_extendedprice * (1 - l_discount))>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0, 1, 2};
    std::vector<LogicalType> key_types = {LogicalType::STRING, LogicalType::STRING, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(filter3);
    filter3->AddChild(join5);
    join5->AddChild(join4);
    join5->AddChild(join3);
    join3->AddChild(join2);
    join3->AddChild(filter2);
    join2->AddChild(project1);
    join2->AddChild(scan_orders);
    project1->AddChild(join1);
    // filter1->AddChild(join1);
    join1->AddChild(scan_nation);
    join1->AddChild(scan_customer);
    filter2->AddChild(scan_lineitem);
    join4->AddChild(scan_nation2);
    join4->AddChild(scan_supplier);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q7执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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