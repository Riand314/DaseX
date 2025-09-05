#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q22=============================
// 测试单线程Q22查询
TEST(TPCHTest, Q22SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
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

    //  ================================================构建Scan================================================
    std::vector<int> customer_ids = {4, 5};
    // <c_phone, c_acctbal>
    std::shared_ptr<PhysicalTableScan> scan_customer1 = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> customer_ids2 = {0, 4, 5};
    // <c_custkey, c_phone, c_acctbal>
    std::shared_ptr<PhysicalTableScan> scan_customer2 = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids2);

    std::vector<int> orders_ids = {1};
    // <o_custkey>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_c_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    Value val_start(0);
    std::shared_ptr<BoundConstantExpression> start_pos = std::make_shared<BoundConstantExpression>(val_start);
    Value val_len(2);
    std::shared_ptr<BoundConstantExpression> len = std::make_shared<BoundConstantExpression>(val_len);

    ScalarProjectFunction bound_function = SubStringFun::GetSubStringFunction();
    std::vector<std::shared_ptr<Expression>> func_arguments = {start_pos, len};
    std::vector<std::shared_ptr<Expression>> arguments = {p1_c_phone, p1_c_phone};
    std::shared_ptr<BoundProjectFunctionExpression> expr1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::STRING, bound_function, arguments, nullptr);
    expr1->bind_info = expr1->function.bind(expr1->function, func_arguments);
    expr1->alias = "cntrycode";

    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_c_acctbal, expr1};
    // <c_acctbal, cntrycode>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> f1_cntrycode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_0(0.0F);
    std::shared_ptr<BoundConstantExpression> right_c_acctbal = std::make_shared<BoundConstantExpression>(value_0);
    std::shared_ptr<BoundComparisonExpression> f1_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f1_c_acctbal, right_c_acctbal);
    std::vector<Value> value_set = {Value("13"), Value("31"), Value("23"), Value("30"), Value("18"), Value("17")};
    std::shared_ptr<BoundInExpression> f1_inExpression = std::make_shared<BoundInExpression>(f1_cntrycode, value_set, LogicalType::STRING);
    std::shared_ptr<BoundConjunctionExpression> f1_conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f1_comparisonExpression, f1_inExpression);
    // <c_acctbal, cntrycode>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(f1_conjunctionExpression);
    filter1->exp_name = "1";

    std::shared_ptr<BoundColumnRefExpression> p2_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_c_acctbal};
    // <c_acctbal>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    //  ================================================构建HashAgg================================================
    std::vector<int> group_item_idxs = {0};
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::AVG };
    // <Avg(c_acctbal)>
    std::shared_ptr<PhysicalHashAgg> hashAgg1 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_c_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);

    ScalarProjectFunction p3_bound_function = SubStringFun::GetSubStringFunction();
    std::vector<std::shared_ptr<Expression>> p3_func_arguments = {start_pos, len};
    std::vector<std::shared_ptr<Expression>> p3_arguments = {p3_c_phone, p3_c_phone};
    std::shared_ptr<BoundProjectFunctionExpression> p3_expr1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::STRING, p3_bound_function, p3_arguments, nullptr);
    p3_expr1->bind_info = p3_expr1->function.bind(p3_expr1->function, p3_func_arguments);
    p3_expr1->alias = "cntrycode";

    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_c_custkey, p3_expr1, p3_c_acctbal};
    // <c_custkey, cntrycode, c_acctbal>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions);
    project1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_cntrycode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundInExpression> f2_inExpression = std::make_shared<BoundInExpression>(f2_cntrycode, value_set, LogicalType::STRING);
    // <c_custkey, cntrycode, c_acctbal>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_inExpression);
    filter2->exp_name = "2";

    //  ================================================构建NestedLoopJoin================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {2};
    std::vector<ExpressionTypes> compare_types = {ExpressionTypes::COMPARE_LESSTHAN};
    // <Avg(c_acctbal), c_custkey, cntrycode, c_acctbal>
    std::shared_ptr<PhysicalNestedLoopJoin> join1 = std::make_shared<PhysicalNestedLoopJoin>(JoinTypes::INNER, build_ids, prode_ids, compare_types);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <Avg(c_acctbal), c_custkey, cntrycode, c_acctbal>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::ANTI, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::shared_ptr<BoundColumnRefExpression> p4_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_cntrycode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_c_acctbal, p4_cntrycode};
    // <c_acctbal, cntrycode>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);
    project4->exp_name = "4";

    // ================================================构建HashAgg================================================
    // 构建HashAgg
    std::vector<int> group_set2 = {1};
    std::vector<int> agg_set2 = {0, 0};
    std::vector<int> star_bitmap2 = {1, 0};
    std::vector<AggFunctionType> aggregate_function_types2 = {AggFunctionType::COUNT, AggFunctionType::SUM};
    // <cntrycode, Count(*), Sum(c_acctbal)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg2 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set2, agg_set2, star_bitmap2, aggregate_function_types2);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg2);
    hashAgg2->AddChild(project4);
    project4->AddChild(join2);
    join2->AddChild(scan_orders);
    join2->AddChild(join1);
    join1->AddChild(hashAgg1);
    join1->AddChild(filter2);
    hashAgg1->AddChild(project2);
    project2->AddChild(filter1);
    filter1->AddChild(project1);
    project1->AddChild(scan_customer1);
    filter2->AddChild(project3);
    project3->AddChild(scan_customer2);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q22执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q22查询
TEST(TPCHTest, Q22FourThreadTest) {
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

    std::cout << "Q22: select\n"
                 "    cntrycode,\n"
                 "    count(*) as numcust,\n"
                 "    sum(c_acctbal) as totacctbal\n"
                 "from (\n"
                 "    select\n"
                 "        substring(c_phone from 1 for 2) as cntrycode,\n"
                 "        c_acctbal\n"
                 "    from\n"
                 "        customer\n"
                 "    where\n"
                 "        substring(c_phone from 1 for 2) in ('13','31','23','30','18','17')\n"
                 "        and c_acctbal > (\n"
                 "            select\n"
                 "                avg(c_acctbal)\n"
                 "            from\n"
                 "                customer\n"
                 "            where\n"
                 "                c_acctbal > 0.00\n"
                 "                and substring(c_phone from 1 for 2) in ('13','31','23','30','18','17')\n"
                 "        )\n"
                 "        and not exists (\n"
                 "            select\n"
                 "                *\n"
                 "            from\n"
                 "                orders\n"
                 "            where\n"
                 "                o_custkey = c_custkey\n"
                 "        )\n"
                 "    ) as custsale\n"
                 "group by\n"
                 "    cntrycode\n"
                 "order by\n"
                 "    cntrycode;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> customer_ids = {4, 5};
    // <c_phone, c_acctbal>
    std::shared_ptr<PhysicalTableScan> scan_customer1 = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> customer_ids2 = {0, 4, 5};
    // <c_custkey, c_phone, c_acctbal>
    std::shared_ptr<PhysicalTableScan> scan_customer2 = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids2);

    std::vector<int> orders_ids = {1};
    // <o_custkey>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_c_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    Value val_start(0);
    std::shared_ptr<BoundConstantExpression> start_pos = std::make_shared<BoundConstantExpression>(val_start);
    Value val_len(2);
    std::shared_ptr<BoundConstantExpression> len = std::make_shared<BoundConstantExpression>(val_len);

    ScalarProjectFunction bound_function = SubStringFun::GetSubStringFunction();
    std::vector<std::shared_ptr<Expression>> func_arguments = {start_pos, len};
    std::vector<std::shared_ptr<Expression>> arguments = {p1_c_phone, p1_c_phone};
    std::shared_ptr<BoundProjectFunctionExpression> expr1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::STRING, bound_function, arguments, nullptr);
    expr1->bind_info = expr1->function.bind(expr1->function, func_arguments);
    expr1->alias = "cntrycode";

    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_c_acctbal, expr1};
    // <c_acctbal, cntrycode>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> f1_cntrycode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_0(0.0F);
    std::shared_ptr<BoundConstantExpression> right_c_acctbal = std::make_shared<BoundConstantExpression>(value_0);
    std::shared_ptr<BoundComparisonExpression> f1_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f1_c_acctbal, right_c_acctbal);
    std::vector<Value> value_set = {Value("13"), Value("31"), Value("23"), Value("30"), Value("18"), Value("17")};
    std::shared_ptr<BoundInExpression> f1_inExpression = std::make_shared<BoundInExpression>(f1_cntrycode, value_set, LogicalType::STRING);
    std::shared_ptr<BoundConjunctionExpression> f1_conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f1_comparisonExpression, f1_inExpression);
    // <c_acctbal, cntrycode>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(f1_conjunctionExpression);
    filter1->exp_name = "1";

    std::shared_ptr<BoundColumnRefExpression> p2_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_c_acctbal};
    // <c_acctbal>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    //  ================================================构建HashAgg================================================
    std::vector<int> group_item_idxs = {0};
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::AVG };
    // <Avg(c_acctbal)>
    std::shared_ptr<PhysicalHashAgg> hashAgg1 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_c_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);

    ScalarProjectFunction p3_bound_function = SubStringFun::GetSubStringFunction();
    std::vector<std::shared_ptr<Expression>> p3_func_arguments = {start_pos, len};
    std::vector<std::shared_ptr<Expression>> p3_arguments = {p3_c_phone, p3_c_phone};
    std::shared_ptr<BoundProjectFunctionExpression> p3_expr1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::STRING, p3_bound_function, p3_arguments, nullptr);
    p3_expr1->bind_info = p3_expr1->function.bind(p3_expr1->function, p3_func_arguments);
    p3_expr1->alias = "cntrycode";

    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_c_custkey, p3_expr1, p3_c_acctbal};
    // <c_custkey, cntrycode, c_acctbal>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions);
    project1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_cntrycode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundInExpression> f2_inExpression = std::make_shared<BoundInExpression>(f2_cntrycode, value_set, LogicalType::STRING);
    // <c_custkey, cntrycode, c_acctbal>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_inExpression);
    filter2->exp_name = "2";

    //  ================================================构建NestedLoopJoin================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {2};
    std::vector<ExpressionTypes> compare_types = {ExpressionTypes::COMPARE_LESSTHAN};
    // <Avg(c_acctbal), c_custkey, cntrycode, c_acctbal>
    std::shared_ptr<PhysicalNestedLoopJoin> join1 = std::make_shared<PhysicalNestedLoopJoin>(JoinTypes::INNER, build_ids, prode_ids, compare_types);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <Avg(c_acctbal), c_custkey, cntrycode, c_acctbal>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::ANTI, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::shared_ptr<BoundColumnRefExpression> p4_c_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_cntrycode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_c_acctbal, p4_cntrycode};
    // <c_acctbal, cntrycode>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);
    project4->exp_name = "4";

    // ================================================构建HashAgg================================================
    // 构建HashAgg
    std::vector<int> group_set2 = {1};
    std::vector<int> agg_set2 = {0, 0};
    std::vector<int> star_bitmap2 = {1, 0};
    std::vector<AggFunctionType> aggregate_function_types2 = {AggFunctionType::COUNT, AggFunctionType::SUM};
    // <cntrycode, Count(*), Sum(c_acctbal)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg2 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set2, agg_set2, star_bitmap2, aggregate_function_types2);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg2);
    hashAgg2->AddChild(project4);
    project4->AddChild(join2);
    join2->AddChild(scan_orders);
    join2->AddChild(join1);
    join1->AddChild(hashAgg1);
    join1->AddChild(filter2);
    hashAgg1->AddChild(project2);
    project2->AddChild(filter1);
    filter1->AddChild(project1);
    project1->AddChild(scan_customer1);
    filter2->AddChild(project3);
    project3->AddChild(scan_customer2);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q22执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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