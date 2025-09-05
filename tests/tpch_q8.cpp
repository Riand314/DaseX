#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q8=============================
// 测试单线程Q8查询
TEST(TPCHTest, Q8SingleThreadTest) {
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

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    InsertPart(table_part, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Region表数据
    std::shared_ptr<Table> table_region;
    InsertRegion(table_region, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertRegion Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> part_ids = {0, 4};
    // <p_partkey, p_type>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> lineitem_ids = {0, 1, 2, 5, 6};
    // <l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> orders_ids = {0, 1, 4};
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> customer_ids = {0, 3};
    // <c_custkey, c_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> nation_ids1 = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation1 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids1);

    std::vector<int> nation_ids2 = {0, 2};
    // <n_nationkey, n_regionkey>
    std::shared_ptr<PhysicalTableScan> scan_nation2 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids2);

    std::vector<int> region_ids = {0, 1};
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalTableScan> scan_region = std::make_shared<PhysicalTableScan>(table_region, -1, region_ids);

    //  ================================================构建Filter-part================================================
    std::shared_ptr<BoundColumnRefExpression> left_p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_ECONOMY("ECONOMY ANODIZED STEEL");
    std::shared_ptr<BoundConstantExpression> right_p_type = std::make_shared<BoundConstantExpression>(value_ECONOMY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_p_type = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_p_type->SetLeft(left_p_type);
    comparisonExpression_p_type->SetRight(right_p_type);
    // <p_partkey, p_type>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_p_type);
    filter1->exp_name = "filter1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids7 = {0};
    std::vector<int> prode_ids7 = {1};
    // <p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join7 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids7, prode_ids7);
    join7->exp_name = "7";
    //  ================================================构建Filter-orders================================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_788889600(788889600);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_788889600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);

    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_851961600(851961600);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate2 = std::make_shared<BoundConstantExpression>(value_851961600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_o_orderdate2->SetLeft(left_o_orderdate2);
    comparisonExpression_o_orderdate2->SetRight(right_o_orderdate2);

    std::shared_ptr<BoundConjunctionExpression> o_orderdate_and = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_o_orderdate, comparisonExpression_o_orderdate2);
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(o_orderdate_and);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids6 = {2};
    std::vector<int> prode_ids6 = {0};
    // <p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join6 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids6, prode_ids6);
    join6->exp_name = "6";

    //  ================================================构建Filter-customer================================================
    std::shared_ptr<BoundColumnRefExpression> left_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(left_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_c_custkey);
    // <c_custkey, c_nationkey>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(comparisonExpression_c_custkey);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids5 = {8};
    std::vector<int> prode_ids5 = {0};
    // <p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids5, prode_ids5);
    join5->exp_name = "5";
    //  ================================================构建Filter-region================================================
    std::shared_ptr<BoundColumnRefExpression> left_r_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_AMERICA("AMERICA");
    std::shared_ptr<BoundConstantExpression> right_r_name = std::make_shared<BoundConstantExpression>(value_AMERICA);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_r_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_r_name->SetLeft(left_r_name);
    comparisonExpression_r_name->SetRight(right_r_name);
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalFilter> filter4 = std::make_shared<PhysicalFilter>(comparisonExpression_r_name);

    std::vector<int> build_ids4 = {1};
    std::vector<int> prode_ids4 = {0};
    // <n_nationkey, n_regionkey, r_regionkey, r_name>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4);
    join4->exp_name = "4";

    std::vector<int> build_ids3 = {0};
    std::vector<int> prode_ids3 = {11};
    // <n_nationkey, n_regionkey, r_regionkey, r_name, p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "3";

    std::vector<int> build_ids2 = {8};
    std::vector<int> prode_ids2 = {0};
    // <n_nationkey, n_regionkey, r_regionkey, r_name, p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate, c_custkey, c_nationkey, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {17};
    // <n_nationkey, n_name, n_nationkey, n_regionkey, r_regionkey, r_name, p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate, c_custkey, c_nationkey, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";
    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 15, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 11, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 12, 0);
    std::shared_ptr<BoundColumnRefExpression> p_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);

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
    extendedprice_discount_1_col->alias = "volume";

    std::vector<LogicalType> arguments3 = { LogicalType::INTEGER, LogicalType::INTEGER };
    scalar_function_p function3 = GetExtractYearFunction();
    ScalarProjectFunction bound_function3("extract_year", arguments3, LogicalType::INTEGER, function3, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions3 = {p_o_orderdate, p_o_orderdate};
    std::shared_ptr<BoundProjectFunctionExpression> extract_year = std::make_shared<BoundProjectFunctionExpression>(LogicalType::INTEGER, bound_function3, expressions3, nullptr);
    extract_year->alias = "o_year";

    std::vector<std::shared_ptr<Expression>> p_expressions = {extract_year, extendedprice_discount_1_col, p_n_name};
    // <o_year, volume, n_name(nation)>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p_expressions);

    // case-when Project
    std::shared_ptr<BoundColumnRefExpression> nation_col = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> volume_col = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    Value value_p("BRAZIL");
    std::shared_ptr<BoundConstantExpression> constant_col = std::make_shared<BoundConstantExpression>(value_p);

    Value value_t(0.0F);
    std::shared_ptr<BoundConstantExpression> constant0_col = std::make_shared<BoundConstantExpression>(value_t);
    std::shared_ptr<BoundComparisonExpression> when_expr = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    when_expr->SetLeft(nation_col);
    when_expr->SetRight(constant_col);

    std::shared_ptr<BoundCaseExpression> case_expr = std::make_shared<BoundCaseExpression>();
    case_expr->SetWhenExpr(when_expr);
    case_expr->SetThenExpr(volume_col);
    case_expr->SetElseExpr(constant0_col);
    case_expr->alias = "CASE_NATION";

    std::shared_ptr<BoundColumnRefExpression> p2_o_year = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);

    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_o_year, case_expr, volume_col};
    // <o_year, case_nation, volume>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);

    // 构建HashAgg
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {1, 2};
    std::vector<int> star_bitmap = {0, 0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM, AggFunctionType::SUM};
    // <o_year, Sum(case_nation), Sum(volume)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_o_year = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_sum_case_nation = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_sum_volume = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 2, 0);

    std::vector<LogicalType> arguments4 = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p function4 = GetDivFunction();
    ScalarProjectFunction bound_function4("/", arguments4, LogicalType::DOUBLE, function4, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_sum_case_nation, p3_sum_volume};
    std::shared_ptr<BoundProjectFunctionExpression> p3_case_nation_div_volume = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, bound_function4, p3_expressions, nullptr);
    p3_case_nation_div_volume->alias = "mkt_share";

    std::vector<std::shared_ptr<Expression>> p3_expressions2 = {p3_o_year, p3_case_nation_div_volume};
    // <o_year, mkt_share>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions2);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(project3);
    project3->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(project1);
    project1->AddChild(join1);
    join1->AddChild(scan_nation1);
    join1->AddChild(join2);
    join2->AddChild(join3);
    join2->AddChild(scan_supplier);
    join3->AddChild(join4);
    join3->AddChild(join5);
    join4->AddChild(scan_nation2);
    join4->AddChild(filter4);
    filter4->AddChild(scan_region);
    join5->AddChild(join6);
    join5->AddChild(filter3);
    filter3->AddChild(scan_customer);
    join6->AddChild(join7);
    join6->AddChild(filter2);
    filter2->AddChild(scan_orders);
    join7->AddChild(filter1);
    join7->AddChild(scan_lineitem);
    filter1->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q8执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q8查询
TEST(TPCHTest, Q8FourThreadTest) {
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
    // spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;
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
    // spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    // spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    };
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;
    // spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Region表数据
    std::shared_ptr<Table> table_region;
    std::vector<std::string> region_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "region.tbl_" + std::to_string(i);
        region_file_names.emplace_back(file_name);
    };
    InsertRegionMul(table_region, scheduler, work_ids, partition_idxs, region_file_names, importFinish);
    importFinish = false;
    // spdlog::info("[{} : {}] InsertRegion Finish!!!", __FILE__, __LINE__);

    std::cout << "Q8: select\n"
                 "    o_year,\n"
                 "    sum(case\n"
                 "    when nation = 'BRAZIL'\n"
                 "    then volume\n"
                 "    else 0\n"
                 "    end) / sum(volume) as mkt_share\n"
                 "from (\n"
                 "    select\n"
                 "        extract(year from o_orderdate) as o_year,\n"
                 "        l_extendedprice * (1-l_discount) as volume,\n"
                 "        n2.n_name as nation\n"
                 "    from\n"
                 "        part,supplier,lineitem,orders,customer,nation n1,nation n2,region\n"
                 "    where\n"
                 "        p_partkey = l_partkey\n"
                 "        and s_suppkey = l_suppkey\n"
                 "        and l_orderkey = o_orderkey\n"
                 "        and o_custkey = c_custkey\n"
                 "        and c_nationkey = n1.n_nationkey\n"
                 "        and n1.n_regionkey = r_regionkey\n"
                 "        and r_name = 'AMERICA'\n"
                 "        and s_nationkey = n2.n_nationkey\n"
                 "        and o_orderdate >= 788889600 and o_orderdate <= 851961600\n"
                 "        and p_type = 'ECONOMY ANODIZED STEEL'\n"
                 ") as all_nations\n"
                 "group by\n"
                 "    o_year\n"
                 "order by\n"
                 "    o_year;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> part_ids = {0, 4};
    // <p_partkey, p_type>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> lineitem_ids = {0, 1, 2, 5, 6};
    // <l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> orders_ids = {0, 1, 4};
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> customer_ids = {0, 3};
    // <c_custkey, c_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> nation_ids1 = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation1 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids1);

    std::vector<int> nation_ids2 = {0, 2};
    // <n_nationkey, n_regionkey>
    std::shared_ptr<PhysicalTableScan> scan_nation2 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids2);

    std::vector<int> region_ids = {0, 1};
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalTableScan> scan_region = std::make_shared<PhysicalTableScan>(table_region, -1, region_ids);

    //  ================================================构建Filter-part================================================
    std::shared_ptr<BoundColumnRefExpression> left_p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_ECONOMY("ECONOMY ANODIZED STEEL");
    std::shared_ptr<BoundConstantExpression> right_p_type = std::make_shared<BoundConstantExpression>(value_ECONOMY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_p_type = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_p_type->SetLeft(left_p_type);
    comparisonExpression_p_type->SetRight(right_p_type);
    // <p_partkey, p_type>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_p_type);
    filter1->exp_name = "filter1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids7 = {0};
    std::vector<int> prode_ids7 = {1};
    // <p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join7 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids7, prode_ids7);
    join7->exp_name = "7";
    //  ================================================构建Filter-orders================================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_788889600(788889600);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_788889600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);

    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_851961600(851961600);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate2 = std::make_shared<BoundConstantExpression>(value_851961600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_o_orderdate2->SetLeft(left_o_orderdate2);
    comparisonExpression_o_orderdate2->SetRight(right_o_orderdate2);

    std::shared_ptr<BoundConjunctionExpression> o_orderdate_and = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_o_orderdate, comparisonExpression_o_orderdate2);
    // <o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(o_orderdate_and);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids6 = {2};
    std::vector<int> prode_ids6 = {0};
    // <p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join6 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids6, prode_ids6);
    join6->exp_name = "6";

    //  ================================================构建Filter-customer================================================
    std::shared_ptr<BoundColumnRefExpression> left_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(left_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_c_custkey);
    // <c_custkey, c_nationkey>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(comparisonExpression_c_custkey);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids5 = {8};
    std::vector<int> prode_ids5 = {0};
    // <p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids5, prode_ids5);
    join5->exp_name = "5";
    //  ================================================构建Filter-region================================================
    std::shared_ptr<BoundColumnRefExpression> left_r_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_AMERICA("AMERICA");
    std::shared_ptr<BoundConstantExpression> right_r_name = std::make_shared<BoundConstantExpression>(value_AMERICA);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_r_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_r_name->SetLeft(left_r_name);
    comparisonExpression_r_name->SetRight(right_r_name);
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalFilter> filter4 = std::make_shared<PhysicalFilter>(comparisonExpression_r_name);

    std::vector<int> build_ids4 = {1};
    std::vector<int> prode_ids4 = {0};
    std::vector<ExpressionTypes> comparison_types4 = {};
    // <n_nationkey, n_regionkey, r_regionkey, r_name>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4, comparison_types4, true);
    join4->exp_name = "4";

    std::vector<int> build_ids3 = {0};
    std::vector<int> prode_ids3 = {11};
    // <n_nationkey, n_regionkey, r_regionkey, r_name, p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate, c_custkey, c_nationkey>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "3";

    std::vector<int> build_ids2 = {8};
    std::vector<int> prode_ids2 = {0};
    // <n_nationkey, n_regionkey, r_regionkey, r_name, p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate, c_custkey, c_nationkey, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {17};
    std::vector<ExpressionTypes> comparison_types1 = {};
    // <n_nationkey, n_name, n_nationkey, n_regionkey, r_regionkey, r_name, p_partkey, p_type, l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, o_custkey, o_orderdate, c_custkey, c_nationkey, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1, comparison_types1, true);
    join1->exp_name = "1";
    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 15, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 11, 0);
    std::shared_ptr<BoundColumnRefExpression> p_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 12, 0);
    std::shared_ptr<BoundColumnRefExpression> p_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);

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
    extendedprice_discount_1_col->alias = "volume";

    std::vector<LogicalType> arguments3 = { LogicalType::INTEGER, LogicalType::INTEGER };
    scalar_function_p function3 = GetExtractYearFunction();
    ScalarProjectFunction bound_function3("extract_year", arguments3, LogicalType::INTEGER, function3, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions3 = {p_o_orderdate, p_o_orderdate};
    std::shared_ptr<BoundProjectFunctionExpression> extract_year = std::make_shared<BoundProjectFunctionExpression>(LogicalType::INTEGER, bound_function3, expressions3, nullptr);
    extract_year->alias = "o_year";

    std::vector<std::shared_ptr<Expression>> p_expressions = {extract_year, extendedprice_discount_1_col, p_n_name};
    // <o_year, volume, n_name(nation)>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p_expressions);

    // case-when Project
    std::shared_ptr<BoundColumnRefExpression> nation_col = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> volume_col = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    Value value_p("BRAZIL");
    std::shared_ptr<BoundConstantExpression> constant_col = std::make_shared<BoundConstantExpression>(value_p);

    Value value_t(0.0F);
    std::shared_ptr<BoundConstantExpression> constant0_col = std::make_shared<BoundConstantExpression>(value_t);
    std::shared_ptr<BoundComparisonExpression> when_expr = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    when_expr->SetLeft(nation_col);
    when_expr->SetRight(constant_col);

    std::shared_ptr<BoundCaseExpression> case_expr = std::make_shared<BoundCaseExpression>();
    case_expr->SetWhenExpr(when_expr);
    case_expr->SetThenExpr(volume_col);
    case_expr->SetElseExpr(constant0_col);
    case_expr->alias = "CASE_NATION";

    std::shared_ptr<BoundColumnRefExpression> p2_o_year = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);

    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_o_year, case_expr, volume_col};
    // <o_year, case_nation, volume>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);

    // 构建HashAgg
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {1, 2};
    std::vector<int> star_bitmap = {0, 0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM, AggFunctionType::SUM};
    // <o_year, Sum(case_nation), Sum(volume)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_o_year = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_sum_case_nation = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_sum_volume = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 2, 0);

    std::vector<LogicalType> arguments4 = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p function4 = GetDivFunction();
    ScalarProjectFunction bound_function4("/", arguments4, LogicalType::DOUBLE, function4, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_sum_case_nation, p3_sum_volume};
    std::shared_ptr<BoundProjectFunctionExpression> p3_case_nation_div_volume = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, bound_function4, p3_expressions, nullptr);
    p3_case_nation_div_volume->alias = "mkt_share";

    std::vector<std::shared_ptr<Expression>> p3_expressions2 = {p3_o_year, p3_case_nation_div_volume};
    // <o_year, mkt_share>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions2);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(project3);
    project3->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(project1);
    project1->AddChild(join1);
    join1->AddChild(scan_nation1);
    join1->AddChild(join2);
    join2->AddChild(join3);
    join2->AddChild(scan_supplier);
    join3->AddChild(join4);
    join3->AddChild(join5);
    join4->AddChild(scan_nation2);
    join4->AddChild(filter4);
    filter4->AddChild(scan_region);
    join5->AddChild(join6);
    join5->AddChild(filter3);
    filter3->AddChild(scan_customer);
    join6->AddChild(join7);
    join6->AddChild(filter2);
    filter2->AddChild(scan_orders);
    join7->AddChild(filter1);
    join7->AddChild(scan_lineitem);
    filter1->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q8执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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