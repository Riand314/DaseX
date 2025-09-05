#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q5=============================
// 测试Q5Pipeline构建
TEST(TPCHTest, Q5SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
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

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    InsertSupplier(table_supplier, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> region_ids = {0, 1};
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalTableScan> scan_region = std::make_shared<PhysicalTableScan>(table_region, -1, region_ids);

    std::vector<int> nation_ids = {0, 1, 2};
    // <n_nationkey, n_name, n_regionkey>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> customer_ids = {0, 3};
    // <c_custkey, c_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> orders_ids = {0, 1, 4};
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> lineitem_ids = {0, 2, 5, 6};
    // <l_orderkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    //  ================================================构建Filter-1================================================
    std::shared_ptr<BoundColumnRefExpression> left_r_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_ASIA("ASIA");
    std::shared_ptr<BoundConstantExpression> right_r_name = std::make_shared<BoundConstantExpression>(value_ASIA);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_r_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_r_name->SetLeft(left_r_name);
    comparisonExpression_r_name->SetRight(right_r_name);
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_r_name);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids5 = {0};
    std::vector<int> prode_ids5 = {2};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids5, prode_ids5);

    std::vector<int> build_ids4 = {2};
    std::vector<int> prode_ids4 = {1};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4);

    //  ================================================构建Filter2================================================
    std::shared_ptr<BoundColumnRefExpression> left_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(left_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_c_custkey);
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(comparisonExpression_c_custkey);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids3 = {5};
    std::vector<int> prode_ids3 = {1};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey, o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);

    std::vector<int> build_ids2 = {7};
    std::vector<int> prode_ids2 = {0};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey, o_orderkey, o_custkey, o_orderdate, l_orderkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);

    std::vector<int> build_ids1 = {1, 1, 0};
    std::vector<int> prode_ids1 = {2, 6, 11};
    // <s_suppkey, s_nationkey, r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey, o_orderkey, o_custkey, o_orderdate, l_orderkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);

    //  ================================================构建Filter-3================================================
    //  =======================================表达1=======================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_820425600(820425600); // 1996-01-01
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_820425600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);
    //  =======================================表达2=======================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_852048000(852048000); // 1997-01-01
    std::shared_ptr<BoundConstantExpression> right_o_orderdate2 = std::make_shared<BoundConstantExpression>(value_852048000);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_o_orderdate2->SetLeft(left_o_orderdate2);
    comparisonExpression_o_orderdate2->SetRight(right_o_orderdate2);
    //  =======================================And表达=======================================
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_o_orderdate, comparisonExpression_o_orderdate2);
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(conjunctionExpression);

    //  ================================================构建Project-1================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 14, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 15, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_extendedprice, p1_l_discount, p1_n_name};
    // <l_extendedprice, l_discount, n_name>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    //  ================================================构建Project-2================================================
    std::shared_ptr<BoundColumnRefExpression> n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);

    Value value_1f(1.0f); // 1997-01-01
    std::shared_ptr<BoundConstantExpression> left_l_discount = std::make_shared<BoundConstantExpression>(value_1f);
    std::shared_ptr<BoundColumnRefExpression> right_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    scalar_function_p sub_function = GetSubFunction();
    std::vector<LogicalType> arguments_type1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    ScalarProjectFunction bound_sub_function("-", arguments_type1, LogicalType::FLOAT, sub_function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions1 = {left_l_discount, right_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_sub_function, expressions1, nullptr);
    discount_1->alias = "1.00 - l_discount";

    std::shared_ptr<BoundColumnRefExpression> l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    std::vector<LogicalType> arguments_type = { LogicalType::INTEGER, LogicalType::FLOAT };
    scalar_function_p mul_function = GetMulFunction();
    ScalarProjectFunction bound_mul_function("*", arguments_type, LogicalType::FLOAT, mul_function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {l_extendedprice, discount_1};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_mul_function, expressions2, nullptr);
    extendedprice_discount_col->alias = "l_extendedprice * (1.00 - l_discount)";
    std::vector<std::shared_ptr<Expression>> expressions3 = {n_name, extendedprice_discount_col};
    // <n_name, l_extendedprice * (1.00 - l_discount)>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(expressions3);

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0};
    std::vector<int32_t> agg_set = {1};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM };
    // <n_name, Sum(l_extendedprice * (1.00 - l_discount))>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {1};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(project1);
    project1->AddChild(join1);
    join1->AddChild(scan_supplier);
    join1->AddChild(join2);
    join2->AddChild(join3);
    join2->AddChild(scan_lineitem);
    join3->AddChild(filter2);
    join3->AddChild(filter3);
    filter2->AddChild(join4);
    filter3->AddChild(scan_orders);
    join4->AddChild(join5);
    join4->AddChild(scan_customer);
    join5->AddChild(filter1);
    join5->AddChild(scan_nation);
    filter1->AddChild(scan_region);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q5执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试Q5Pipeline构建
TEST(TPCHTest, Q5FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
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
    // spdlog::info("[{} : {}] Import Data Finish!!!", __FILE__, __LINE__);

    std::cout << "Q5: select\n"
                 "    n_name,\n"
                 "    sum(l_extendedprice * (1 - l_discount)) as revenue \n"
                 "from\n"
                 "    customer,orders,lineitem,supplier,nation,region \n"
                 "where\n"
                 "    c_custkey = o_custkey\n"
                 "    and l_orderkey = o_orderkey\n"
                 "    and l_suppkey = s_suppkey\n"
                 "    and c_nationkey = s_nationkey\n"
                 "    and s_nationkey = n_nationkey\n"
                 "    and n_regionkey = r_regionkey\n"
                 "    and r_name = 'ASIA' \n"
                 "    and o_orderdate >= date 820425600\n"
                 "    and o_orderdate < date 852048000 \n"
                 "group by\n"
                 "    n_name\n"
                 "order by\n"
                 "    revenue desc;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> region_ids = {0, 1};
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalTableScan> scan_region = std::make_shared<PhysicalTableScan>(table_region, -1, region_ids);

    std::vector<int> nation_ids = {0, 1, 2};
    // <n_nationkey, n_name, n_regionkey>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> customer_ids = {0, 3};
    // <c_custkey, c_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> orders_ids = {0, 1, 4};
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> lineitem_ids = {0, 2, 5, 6};
    // <l_orderkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    //  ================================================构建Filter-1================================================
    std::shared_ptr<BoundColumnRefExpression> left_r_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_ASIA("ASIA");
    std::shared_ptr<BoundConstantExpression> right_r_name = std::make_shared<BoundConstantExpression>(value_ASIA);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_r_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_r_name->SetLeft(left_r_name);
    comparisonExpression_r_name->SetRight(right_r_name);
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_r_name);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids5 = {0};
    std::vector<int> prode_ids5 = {2};
    std::vector<ExpressionTypes> comparison_types = {};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids5, prode_ids5, comparison_types, true);

    std::vector<int> build_ids4 = {2};
    std::vector<int> prode_ids4 = {1};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4);

    //  ================================================构建Filter2================================================
    std::shared_ptr<BoundColumnRefExpression> left_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(left_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_c_custkey);
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(comparisonExpression_c_custkey);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids3 = {5};
    std::vector<int> prode_ids3 = {1};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey, o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);

    std::vector<int> build_ids2 = {7};
    std::vector<int> prode_ids2 = {0};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey, o_orderkey, o_custkey, o_orderdate, l_orderkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);

    std::vector<int> build_ids1 = {1, 1, 0};
    std::vector<int> prode_ids1 = {2, 6, 11};
    // <s_suppkey, s_nationkey, r_regionkey, r_name, n_nationkey, n_name, n_regionkey, c_custkey, c_nationkey, o_orderkey, o_custkey, o_orderdate, l_orderkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);

    //  ================================================构建Filter-3================================================
    //  =======================================表达1=======================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_820425600(820425600); // 1996-01-01
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_820425600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);
    //  =======================================表达2=======================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_852048000(852048000); // 1997-01-01
    std::shared_ptr<BoundConstantExpression> right_o_orderdate2 = std::make_shared<BoundConstantExpression>(value_852048000);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_o_orderdate2->SetLeft(left_o_orderdate2);
    comparisonExpression_o_orderdate2->SetRight(right_o_orderdate2);
    //  =======================================And表达=======================================
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_o_orderdate, comparisonExpression_o_orderdate2);
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(conjunctionExpression);

    //  ================================================构建Project-1================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 14, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 15, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_extendedprice, p1_l_discount, p1_n_name};
    // <l_extendedprice, l_discount, n_name>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    //  ================================================构建Project-2================================================
    std::shared_ptr<BoundColumnRefExpression> n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);

    Value value_1f(1.0f); // 1997-01-01
    std::shared_ptr<BoundConstantExpression> left_l_discount = std::make_shared<BoundConstantExpression>(value_1f);
    std::shared_ptr<BoundColumnRefExpression> right_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    scalar_function_p sub_function = GetSubFunction();
    std::vector<LogicalType> arguments_type1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    ScalarProjectFunction bound_sub_function("-", arguments_type1, LogicalType::FLOAT, sub_function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions1 = {left_l_discount, right_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_sub_function, expressions1, nullptr);
    discount_1->alias = "1.00 - l_discount";

    std::shared_ptr<BoundColumnRefExpression> l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    std::vector<LogicalType> arguments_type = { LogicalType::INTEGER, LogicalType::FLOAT };
    scalar_function_p mul_function = GetMulFunction();
    ScalarProjectFunction bound_mul_function("*", arguments_type, LogicalType::FLOAT, mul_function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {l_extendedprice, discount_1};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_mul_function, expressions2, nullptr);
    extendedprice_discount_col->alias = "l_extendedprice * (1.00 - l_discount)";
    std::vector<std::shared_ptr<Expression>> expressions3 = {n_name, extendedprice_discount_col};
    // <n_name, l_extendedprice * (1.00 - l_discount)>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(expressions3);

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0};
    std::vector<int32_t> agg_set = {1};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM };
    // <n_name, Sum(l_extendedprice * (1.00 - l_discount))>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {1};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(project1);
    project1->AddChild(join1);
    join1->AddChild(scan_supplier);
    join1->AddChild(join2);
    join2->AddChild(join3);
    join2->AddChild(scan_lineitem);
    join3->AddChild(filter2);
    join3->AddChild(filter3);
    filter2->AddChild(join4);
    filter3->AddChild(scan_orders);
    join4->AddChild(join5);
    join4->AddChild(scan_customer);
    join5->AddChild(filter1);
    join5->AddChild(scan_nation);
    filter1->AddChild(scan_region);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    // auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    // auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q5执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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
