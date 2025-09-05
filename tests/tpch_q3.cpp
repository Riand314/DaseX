#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q3=============================
// 测试单线程Q3查询
TEST(TPCHTest, Q3SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    InsertCustomer(table_customer, scheduler, fileDir, importFinish);
    importFinish = false;

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    InsertOrders(table_orders, scheduler, fileDir, importFinish);
    importFinish = false;

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;

    //  ================================================构建Scan================================================
    std::vector<int> customer_ids = {0, 6};
    // <c_custkey, c_mktsegment>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> orders_ids = {0, 1, 4, 7};
    // <o_orderkey, o_custkey, o_orderdate, o_shippriority>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> lineitem_ids = {0, 5, 6, 10};
    // <l_orderkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    //  ================================================构建Filter-customer================================================
    std::shared_ptr<BoundColumnRefExpression> left_c_mktsegment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_BUILDING("BUILDING");
    std::shared_ptr<BoundConstantExpression> right_c_mktsegment = std::make_shared<BoundConstantExpression>(value_BUILDING);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_mktsegment = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_c_mktsegment->SetLeft(left_c_mktsegment);
    comparisonExpression_c_mktsegment->SetRight(right_c_mktsegment);

    std::shared_ptr<BoundColumnRefExpression> left_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(left_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_c_custkey);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_customer = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_c_mktsegment, comparisonExpression_c_custkey);
    // <c_custkey, c_mktsegment>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression_customer);

    //  ================================================构建Filter-orders================================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_795196800(795196800);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_795196800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);
    // <o_orderkey, o_custkey, o_orderdate, o_shippriority>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(comparisonExpression_o_orderdate);

    //  ================================================构建Filter-Lineitem================================================
    std::shared_ptr<BoundColumnRefExpression> left_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate = std::make_shared<BoundConstantExpression>(value_795196800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN);
    comparisonExpression_l_shipdate->SetLeft(left_l_shipdate);
    comparisonExpression_l_shipdate->SetRight(right_l_shipdate);
    // <l_orderkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(comparisonExpression_l_shipdate);

    //  ================================================构建HashJoin ===================================================
    // Customer join Orders
    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <c_custkey, c_mktsegment, o_orderkey, o_custkey, o_orderdate, o_shippriority>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);

    // Last join Lineitem
    std::vector<int> build_ids1 = {2};
    std::vector<int> prode_ids1 = {0};
    // <c_custkey, c_mktsegment, o_orderkey, o_custkey, o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 8, 0);
    Value value_1F(1.0F);
    std::shared_ptr<BoundConstantExpression> l_constant_1 = std::make_shared<BoundConstantExpression>(value_1F);
    std::vector<LogicalType> arguments_type1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p sub_function = GetSubFunction();
    ScalarProjectFunction bound_sub_function("-", arguments_type1, LogicalType::FLOAT, sub_function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions1 = {l_constant_1, l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> l_discount_sub_1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_sub_function, expressions1, nullptr);
    l_discount_sub_1->alias = "(1.00 - l_discount)";

    std::shared_ptr<BoundColumnRefExpression> l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);
    std::vector<LogicalType> arguments_type2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p mul_function = GetMulFunction();
    ScalarProjectFunction bound_mul_function("*", arguments_type2, LogicalType::FLOAT, mul_function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {l_extendedprice, l_discount_sub_1};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_mul_function, expressions2, nullptr);
    extendedprice_discount_col->alias = "l_extendedprice * (1.00 - l_discount)";

    std::shared_ptr<BoundColumnRefExpression> p_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p_o_shippriority = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::vector<std::shared_ptr<Expression>> p_expressions = {p_l_orderkey, extendedprice_discount_col, p_o_orderdate, p_o_shippriority};
    // <l_orderkey, l_extendedprice*(1-l_discount), o_orderdate, o_shippriority>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p_expressions);

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0, 2, 3}; // GroupBy字段
    std::vector<int> agg_set = {1}; // 聚合字段
    std::vector<int> star_bitmap = {0}; // 对应字段是否为 ‘*’ 表
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM };
    // <l_orderkey, o_orderdate, o_shippriority, Sum(l_extendedprice*(1-l_discount))>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {3, 1};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project1);
    project1->AddChild(join1);
    join1->AddChild(join2);
    join1->AddChild(filter3);
    join2->AddChild(filter1);
    join2->AddChild(filter2);
    filter1->AddChild(scan_customer);
    filter2->AddChild(scan_orders);
    filter3->AddChild(scan_lineitem);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q3执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q3查询
TEST(TPCHTest, Q3FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
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

    std::cout << "Q3: select\n"
                 "    l_orderkey,\n"
                 "    sum(l_extendedprice*(1-l_discount)) as revenue,\n"
                 "    o_orderdate,\n"
                 "    o_shippriority\n"
                 "from\n"
                 "    customer, orders, lineitem\n"
                 "where\n"
                 "    c_mktsegment = 'BUILDING'\n"
                 "    and c_custkey = o_custkey\n"
                 "    and l_orderkey = o_orderkey\n"
                 "    and o_orderdate < 795196800 \n"
                 "    and l_shipdate > 795196800\n"
                 "group by \n"
                 "    l_orderkey, \n"
                 "    o_orderdate, \n"
                 "    o_shippriority \n"
                 "order by\n"
                 "    revenue desc, \n"
                 "    o_orderdate;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> customer_ids = {0, 6};
    // <c_custkey, c_mktsegment>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> orders_ids = {0, 1, 4, 7};
    // <o_orderkey, o_custkey, o_orderdate, o_shippriority>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> lineitem_ids = {0, 5, 6, 10};
    // <l_orderkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    //  ================================================构建Filter-customer================================================
    std::shared_ptr<BoundColumnRefExpression> left_c_mktsegment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_BUILDING("BUILDING");
    std::shared_ptr<BoundConstantExpression> right_c_mktsegment = std::make_shared<BoundConstantExpression>(value_BUILDING);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_mktsegment = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_c_mktsegment->SetLeft(left_c_mktsegment);
    comparisonExpression_c_mktsegment->SetRight(right_c_mktsegment);

    std::shared_ptr<BoundColumnRefExpression> left_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_c_custkey = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_c_custkey->SetLeft(left_c_custkey);
    comparisonExpression_c_custkey->SetRight(right_c_custkey);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_customer = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_c_mktsegment, comparisonExpression_c_custkey);
    // <c_custkey, c_mktsegment>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression_customer);

    //  ================================================构建Filter-orders================================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_795196800(795196800);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_795196800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);
    // <o_orderkey, o_custkey, o_orderdate, o_shippriority>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(comparisonExpression_o_orderdate);

    //  ================================================构建Filter-Lineitem================================================
    std::shared_ptr<BoundColumnRefExpression> left_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate = std::make_shared<BoundConstantExpression>(value_795196800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN);
    comparisonExpression_l_shipdate->SetLeft(left_l_shipdate);
    comparisonExpression_l_shipdate->SetRight(right_l_shipdate);
    // <l_orderkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(comparisonExpression_l_shipdate);

    //  ================================================构建HashJoin ===================================================
    // Customer join Orders
    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <c_custkey, c_mktsegment, o_orderkey, o_custkey, o_orderdate, o_shippriority>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);

    // Last join Lineitem
    std::vector<int> build_ids1 = {2};
    std::vector<int> prode_ids1 = {0};
    // <c_custkey, c_mktsegment, o_orderkey, o_custkey, o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 8, 0);
    Value value_1F(1.0F);
    std::shared_ptr<BoundConstantExpression> l_constant_1 = std::make_shared<BoundConstantExpression>(value_1F);
    std::vector<LogicalType> arguments_type1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p sub_function = GetSubFunction();
    ScalarProjectFunction bound_sub_function("-", arguments_type1, LogicalType::FLOAT, sub_function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions1 = {l_constant_1, l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> l_discount_sub_1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_sub_function, expressions1, nullptr);
    l_discount_sub_1->alias = "(1.00 - l_discount)";

    std::shared_ptr<BoundColumnRefExpression> l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);
    std::vector<LogicalType> arguments_type2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p mul_function = GetMulFunction();
    ScalarProjectFunction bound_mul_function("*", arguments_type2, LogicalType::FLOAT, mul_function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {l_extendedprice, l_discount_sub_1};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_mul_function, expressions2, nullptr);
    extendedprice_discount_col->alias = "l_extendedprice * (1.00 - l_discount)";

    std::shared_ptr<BoundColumnRefExpression> p_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p_o_shippriority = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::vector<std::shared_ptr<Expression>> p_expressions = {p_l_orderkey, extendedprice_discount_col, p_o_orderdate, p_o_shippriority};
    // <l_orderkey, l_extendedprice*(1-l_discount), o_orderdate, o_shippriority>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p_expressions);

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0, 2, 3}; // GroupBy字段
    std::vector<int> agg_set = {1}; // 聚合字段
    std::vector<int> star_bitmap = {0}; // 对应字段是否为 ‘*’ 表
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM };
    // <l_orderkey, o_orderdate, o_shippriority, Sum(l_extendedprice*(1-l_discount))>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {3, 1};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project1);
    project1->AddChild(join1);
    join1->AddChild(join2);
    join1->AddChild(filter3);
    join2->AddChild(filter1);
    join2->AddChild(filter2);
    filter1->AddChild(scan_customer);
    filter2->AddChild(scan_orders);
    filter3->AddChild(scan_lineitem);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q3执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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
