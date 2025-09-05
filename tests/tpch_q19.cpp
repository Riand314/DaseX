#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q19=============================
// 测试单线程Q19查询
TEST(TPCHTest, Q19SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    InsertPart(table_part, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids = {1,4,5,6,13,14};
    // <l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> part_ids = {0,3,5,6};
    // <p_partkey, p_brand, p_size, p_container>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_l_shipinstruct = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    Value value_1("DELIVER IN PERSON");
    std::shared_ptr<BoundConstantExpression> right_l_shipinstruct = std::make_shared<BoundConstantExpression>(value_1);
    std::shared_ptr<BoundComparisonExpression> f1_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f1_l_shipinstruct, right_l_shipinstruct);
    // <l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(f1_comparisonExpression);
    filter1->exp_name = "1";

    std::shared_ptr<BoundColumnRefExpression> f2_l_shipmode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    Value value_2("AIR");
    std::shared_ptr<BoundConstantExpression> right_l_shipmode = std::make_shared<BoundConstantExpression>(value_2);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f2_l_shipmode, right_l_shipmode);
    std::shared_ptr<BoundColumnRefExpression> f2_l_shipmode2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    Value value_3("AIR REG");
    std::shared_ptr<BoundConstantExpression> right_l_shipmode2 = std::make_shared<BoundConstantExpression>(value_3);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f2_l_shipmode2, right_l_shipmode2);
    std::shared_ptr<BoundConjunctionExpression> f2_conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, f2_comparisonExpression, f2_comparisonExpression2);
    // <l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_conjunctionExpression);
    filter2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 3, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_partkey,p1_l_quantity,p1_l_extendedprice,p1_l_discount};
    // <l_partkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {0};
    // <p_partkey, p_brand, p_size, p_container, l_partkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids, prode_ids);
    join1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f3_p_brand1 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_Brand_12("Brand#12");
    std::shared_ptr<BoundConstantExpression> right_p_brand1 = std::make_shared<BoundConstantExpression>(value_Brand_12);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression1 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f3_p_brand1, right_p_brand1);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity1 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_1f(1.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity1 = std::make_shared<BoundConstantExpression>(value_1f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_l_quantity1, right_l_quantity1);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity2 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_11f(11.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity2 = std::make_shared<BoundConstantExpression>(value_11f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_l_quantity2, right_l_quantity2);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size1 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_1l(1);
    std::shared_ptr<BoundConstantExpression> right_p_size1 = std::make_shared<BoundConstantExpression>(value_1l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression4 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_p_size1, right_p_size1);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_5l(5);
    std::shared_ptr<BoundConstantExpression> right_p_size2 = std::make_shared<BoundConstantExpression>(value_5l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression5 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_p_size2, right_p_size2);

    Value value_SM1("SM CASE");
    Value value_SM2("SM BOX");
    Value value_SM3("SM PACK");
    Value value_SM4("SM PKG");
    std::vector<Value> value_SM_set = {value_SM1, value_SM2, value_SM3, value_SM4};
    std::shared_ptr<BoundColumnRefExpression> f3_p_container = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundInExpression> f3_inExpression1 = std::make_shared<BoundInExpression>(f3_p_container, value_SM_set, LogicalType::STRING);

    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression2, f3_comparisonExpression3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression4, f3_comparisonExpression5);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression1, f3_conjunctionExpression2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression4 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression3, f3_inExpression1);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression5 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression1, f3_conjunctionExpression4);
    f3_conjunctionExpression5->alias = "1";
    std::shared_ptr<BoundColumnRefExpression> f3_p_brand1_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_Brand_23("Brand#23");
    std::shared_ptr<BoundConstantExpression> right_p_brand1_2 = std::make_shared<BoundConstantExpression>(value_Brand_23);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression1_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f3_p_brand1_2, right_p_brand1_2);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity1_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_10f(10.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity1_2 = std::make_shared<BoundConstantExpression>(value_10f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression2_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_l_quantity1_2, right_l_quantity1_2);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity2_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_20f(20.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity2_2 = std::make_shared<BoundConstantExpression>(value_20f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression3_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_l_quantity2_2, right_l_quantity2_2);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size1_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundConstantExpression> right_p_size1_2 = std::make_shared<BoundConstantExpression>(value_1l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression4_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_p_size1_2, right_p_size1_2);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size2_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_10l(10);
    std::shared_ptr<BoundConstantExpression> right_p_size2_2 = std::make_shared<BoundConstantExpression>(value_10l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression5_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_p_size2_2, right_p_size2_2);

    Value value_MED1("MED BAG");
    Value value_MED2("MED BOX");
    Value value_MED3("MED PACK");
    Value value_MED4("MED PKG");
    std::vector<Value> value_MED_set = {value_MED1, value_MED2, value_MED3, value_MED4};
    std::shared_ptr<BoundColumnRefExpression> f3_p_container_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundInExpression> f3_inExpression1_2 = std::make_shared<BoundInExpression>(f3_p_container_2, value_MED_set, LogicalType::STRING);

    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression1_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression2_2, f3_comparisonExpression3_2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression2_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression4_2, f3_comparisonExpression5_2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression3_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression1_2, f3_conjunctionExpression2_2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression4_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression3_2, f3_inExpression1_2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression5_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression1_2, f3_conjunctionExpression4_2);
    f3_conjunctionExpression5_2->alias = "2";
    std::shared_ptr<BoundColumnRefExpression> f3_p_brand1_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_Brand_34("Brand#34");
    std::shared_ptr<BoundConstantExpression> right_p_brand1_3 = std::make_shared<BoundConstantExpression>(value_Brand_34);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression1_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f3_p_brand1_3, right_p_brand1_3);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity1_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    std::shared_ptr<BoundConstantExpression> right_l_quantity1_3 = std::make_shared<BoundConstantExpression>(value_20f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression2_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_l_quantity1_3, right_l_quantity1_3);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity2_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_30f(30.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity2_3 = std::make_shared<BoundConstantExpression>(value_30f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression3_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_l_quantity2_3, right_l_quantity2_3);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size1_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundConstantExpression> right_p_size1_3 = std::make_shared<BoundConstantExpression>(value_1l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression4_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_p_size1_3, right_p_size1_3);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size2_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_15l(15);
    std::shared_ptr<BoundConstantExpression> right_p_size2_3 = std::make_shared<BoundConstantExpression>(value_15l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression5_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_p_size2_3, right_p_size2_3);

    Value value_LG1("LG CASE");
    Value value_LG2("LG BOX");
    Value value_LG3("LG PACK");
    Value value_LG4("LG PKG");
    std::vector<Value> value_LG_set = {value_LG1, value_LG2, value_LG3, value_LG4};
    std::shared_ptr<BoundColumnRefExpression> f3_p_container_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundInExpression> f3_inExpression1_3 = std::make_shared<BoundInExpression>(f3_p_container_3, value_LG_set, LogicalType::STRING);

    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression1_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression2_3, f3_comparisonExpression3_3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression2_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression4_3, f3_comparisonExpression5_3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression3_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression1_3, f3_conjunctionExpression2_3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression4_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression3_3, f3_inExpression1_3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression5_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression1_3, f3_conjunctionExpression4_3);
    f3_conjunctionExpression5_3->alias = "3";
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression6 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, f3_conjunctionExpression5, f3_conjunctionExpression5_2);
    f3_conjunctionExpression6->alias = "4";
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression7 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, f3_conjunctionExpression6, f3_conjunctionExpression5_3);
    f3_conjunctionExpression7->alias = "5";
    // <p_partkey, p_brand, p_size, p_container, l_partkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(f3_conjunctionExpression7);
    // std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(f3_conjunctionExpression5_3);
    filter3->exp_name = "3";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);

    std::vector<LogicalType> p2_arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p2_function1 = GetSubFunction();
    ScalarProjectFunction p2_bound_function1("-", p2_arguments1, LogicalType::FLOAT, p2_function1, nullptr);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1f);
    std::vector<std::shared_ptr<Expression>> p2_expressions1 = {constant1_col, p2_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p2_bound_function1, p2_expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> p2_arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p2_function2 = GetMulFunction();
    ScalarProjectFunction p2_bound_function2("*", p2_arguments2, LogicalType::FLOAT, p2_function2, nullptr);
    std::vector<std::shared_ptr<Expression>> p2_expressions2 = {p2_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p2_bound_function2, p2_expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice * (1 - l_discount)";

    std::vector<std::shared_ptr<Expression>> p2_expressions = {extendedprice_discount_1_col};
    // <l_extendedprice * (1 - l_discount)>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    // ================================================构建HashAgg================================================
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM};
    std::vector<int32_t> group_item_idxs = {0};
    // <Sum(l_extendedprice * (1 - l_discount))>
    std::shared_ptr<PhysicalHashAgg> hashAgg = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);


    hashAgg->AddChild(project2);
    project2->AddChild(filter3);
    filter3->AddChild(join1);
    join1->AddChild(scan_part);
    join1->AddChild(project1);
    project1->AddChild(filter2);
    filter2->AddChild(filter1);
    filter1->AddChild(scan_lineitem);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    hashAgg->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q19执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q19查询
TEST(TPCHTest, Q19FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
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
    // spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

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

    std::cout << "Q19: select\n"
                 "    sum(l_extendedprice * (1 - l_discount) ) as revenue\n"
                 "from\n"
                 "    lineitem, part\n"
                 "where (\n"
                 "        p_partkey = l_partkey\n"
                 "        and p_brand = 'Brand#12'\n"
                 "        and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
                 "        and l_quantity >= 1 and l_quantity <= 11\n"
                 "        and p_size >= 1 and p_size <=5\n"
                 "        and l_shipmode in ('AIR', 'AIR REG')\n"
                 "        and l_shipinstruct = 'DELIVER IN PERSON'\n"
                 "    ) or (\n"
                 "        p_partkey = l_partkey\n"
                 "        and p_brand = 'Brand#23'\n"
                 "        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
                 "        and l_quantity >= 10 and l_quantity <= 20\n"
                 "        and p_size >= 1 and p_size <= 10\n"
                 "        and l_shipmode in ('AIR', 'AIR REG')\n"
                 "        and l_shipinstruct = 'DELIVER IN PERSON'\n"
                 "    ) or (\n"
                 "        p_partkey = l_partkey\n"
                 "        and p_brand = 'Brand#34'\n"
                 "        and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
                 "        and l_quantity >= 20 and l_quantity <= 30\n"
                 "        and p_size >= 1 and p_size <= 15\n"
                 "        and l_shipmode in ('AIR', 'AIR REG')\n"
                 "        and l_shipinstruct = 'DELIVER IN PERSON'\n"
                 "    );" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids = {1,4,5,6,13,14};
    // <l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> part_ids = {0,3,5,6};
    // <p_partkey, p_brand, p_size, p_container>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_l_shipinstruct = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    Value value_1("DELIVER IN PERSON");
    std::shared_ptr<BoundConstantExpression> right_l_shipinstruct = std::make_shared<BoundConstantExpression>(value_1);
    std::shared_ptr<BoundComparisonExpression> f1_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f1_l_shipinstruct, right_l_shipinstruct);
    // <l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(f1_comparisonExpression);
    filter1->exp_name = "1";

    std::shared_ptr<BoundColumnRefExpression> f2_l_shipmode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    Value value_2("AIR");
    std::shared_ptr<BoundConstantExpression> right_l_shipmode = std::make_shared<BoundConstantExpression>(value_2);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f2_l_shipmode, right_l_shipmode);
    std::shared_ptr<BoundColumnRefExpression> f2_l_shipmode2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    Value value_3("AIR REG");
    std::shared_ptr<BoundConstantExpression> right_l_shipmode2 = std::make_shared<BoundConstantExpression>(value_3);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f2_l_shipmode2, right_l_shipmode2);
    std::shared_ptr<BoundConjunctionExpression> f2_conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, f2_comparisonExpression, f2_comparisonExpression2);
    // <l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_conjunctionExpression);
    filter2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 3, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_partkey,p1_l_quantity,p1_l_extendedprice,p1_l_discount};
    // <l_partkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {0};
    // <p_partkey, p_brand, p_size, p_container, l_partkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids, prode_ids);
    join1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f3_p_brand1 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_Brand_12("Brand#12");
    std::shared_ptr<BoundConstantExpression> right_p_brand1 = std::make_shared<BoundConstantExpression>(value_Brand_12);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression1 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f3_p_brand1, right_p_brand1);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity1 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_1f(1.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity1 = std::make_shared<BoundConstantExpression>(value_1f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_l_quantity1, right_l_quantity1);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity2 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_11f(11.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity2 = std::make_shared<BoundConstantExpression>(value_11f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_l_quantity2, right_l_quantity2);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size1 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_1l(1);
    std::shared_ptr<BoundConstantExpression> right_p_size1 = std::make_shared<BoundConstantExpression>(value_1l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression4 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_p_size1, right_p_size1);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_5l(5);
    std::shared_ptr<BoundConstantExpression> right_p_size2 = std::make_shared<BoundConstantExpression>(value_5l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression5 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_p_size2, right_p_size2);

    Value value_SM1("SM CASE");
    Value value_SM2("SM BOX");
    Value value_SM3("SM PACK");
    Value value_SM4("SM PKG");
    std::vector<Value> value_SM_set = {value_SM1, value_SM2, value_SM3, value_SM4};
    std::shared_ptr<BoundColumnRefExpression> f3_p_container = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundInExpression> f3_inExpression1 = std::make_shared<BoundInExpression>(f3_p_container, value_SM_set, LogicalType::STRING);

    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression2, f3_comparisonExpression3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression4, f3_comparisonExpression5);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression1, f3_conjunctionExpression2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression4 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression3, f3_inExpression1);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression5 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression1, f3_conjunctionExpression4);
    f3_conjunctionExpression5->alias = "1";
    std::shared_ptr<BoundColumnRefExpression> f3_p_brand1_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_Brand_23("Brand#23");
    std::shared_ptr<BoundConstantExpression> right_p_brand1_2 = std::make_shared<BoundConstantExpression>(value_Brand_23);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression1_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f3_p_brand1_2, right_p_brand1_2);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity1_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_10f(10.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity1_2 = std::make_shared<BoundConstantExpression>(value_10f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression2_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_l_quantity1_2, right_l_quantity1_2);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity2_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_20f(20.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity2_2 = std::make_shared<BoundConstantExpression>(value_20f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression3_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_l_quantity2_2, right_l_quantity2_2);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size1_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundConstantExpression> right_p_size1_2 = std::make_shared<BoundConstantExpression>(value_1l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression4_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_p_size1_2, right_p_size1_2);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size2_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_10l(10);
    std::shared_ptr<BoundConstantExpression> right_p_size2_2 = std::make_shared<BoundConstantExpression>(value_10l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression5_2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_p_size2_2, right_p_size2_2);

    Value value_MED1("MED BAG");
    Value value_MED2("MED BOX");
    Value value_MED3("MED PACK");
    Value value_MED4("MED PKG");
    std::vector<Value> value_MED_set = {value_MED1, value_MED2, value_MED3, value_MED4};
    std::shared_ptr<BoundColumnRefExpression> f3_p_container_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundInExpression> f3_inExpression1_2 = std::make_shared<BoundInExpression>(f3_p_container_2, value_MED_set, LogicalType::STRING);

    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression1_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression2_2, f3_comparisonExpression3_2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression2_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression4_2, f3_comparisonExpression5_2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression3_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression1_2, f3_conjunctionExpression2_2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression4_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression3_2, f3_inExpression1_2);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression5_2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression1_2, f3_conjunctionExpression4_2);
    f3_conjunctionExpression5_2->alias = "2";
    std::shared_ptr<BoundColumnRefExpression> f3_p_brand1_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_Brand_34("Brand#34");
    std::shared_ptr<BoundConstantExpression> right_p_brand1_3 = std::make_shared<BoundConstantExpression>(value_Brand_34);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression1_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f3_p_brand1_3, right_p_brand1_3);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity1_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    std::shared_ptr<BoundConstantExpression> right_l_quantity1_3 = std::make_shared<BoundConstantExpression>(value_20f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression2_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_l_quantity1_3, right_l_quantity1_3);

    std::shared_ptr<BoundColumnRefExpression> f3_l_quantity2_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 5, 0);
    Value value_30f(30.0f);
    std::shared_ptr<BoundConstantExpression> right_l_quantity2_3 = std::make_shared<BoundConstantExpression>(value_30f);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression3_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_l_quantity2_3, right_l_quantity2_3);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size1_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundConstantExpression> right_p_size1_3 = std::make_shared<BoundConstantExpression>(value_1l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression4_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f3_p_size1_3, right_p_size1_3);

    std::shared_ptr<BoundColumnRefExpression> f3_p_size2_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    Value value_15l(15);
    std::shared_ptr<BoundConstantExpression> right_p_size2_3 = std::make_shared<BoundConstantExpression>(value_15l);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression5_3 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f3_p_size2_3, right_p_size2_3);

    Value value_LG1("LG CASE");
    Value value_LG2("LG BOX");
    Value value_LG3("LG PACK");
    Value value_LG4("LG PKG");
    std::vector<Value> value_LG_set = {value_LG1, value_LG2, value_LG3, value_LG4};
    std::shared_ptr<BoundColumnRefExpression> f3_p_container_3 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundInExpression> f3_inExpression1_3 = std::make_shared<BoundInExpression>(f3_p_container_3, value_LG_set, LogicalType::STRING);

    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression1_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression2_3, f3_comparisonExpression3_3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression2_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression4_3, f3_comparisonExpression5_3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression3_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression1_3, f3_conjunctionExpression2_3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression4_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_conjunctionExpression3_3, f3_inExpression1_3);
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression5_3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f3_comparisonExpression1_3, f3_conjunctionExpression4_3);
    f3_conjunctionExpression5_3->alias = "3";
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression6 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, f3_conjunctionExpression5, f3_conjunctionExpression5_2);
    f3_conjunctionExpression6->alias = "4";
    std::shared_ptr<BoundConjunctionExpression> f3_conjunctionExpression7 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, f3_conjunctionExpression6, f3_conjunctionExpression5_3);
    f3_conjunctionExpression7->alias = "5";
    // <p_partkey, p_brand, p_size, p_container, l_partkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(f3_conjunctionExpression7);
    // std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(f3_conjunctionExpression5_3);
    filter3->exp_name = "3";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);

    std::vector<LogicalType> p2_arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p2_function1 = GetSubFunction();
    ScalarProjectFunction p2_bound_function1("-", p2_arguments1, LogicalType::FLOAT, p2_function1, nullptr);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1f);
    std::vector<std::shared_ptr<Expression>> p2_expressions1 = {constant1_col, p2_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p2_bound_function1, p2_expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> p2_arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p2_function2 = GetMulFunction();
    ScalarProjectFunction p2_bound_function2("*", p2_arguments2, LogicalType::FLOAT, p2_function2, nullptr);
    std::vector<std::shared_ptr<Expression>> p2_expressions2 = {p2_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p2_bound_function2, p2_expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice * (1 - l_discount)";

    std::vector<std::shared_ptr<Expression>> p2_expressions = {extendedprice_discount_1_col};
    // <l_extendedprice * (1 - l_discount)>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    // ================================================构建HashAgg================================================
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM};
    std::vector<int32_t> group_item_idxs = {0};
    // <Sum(l_extendedprice * (1 - l_discount))>
    std::shared_ptr<PhysicalHashAgg> hashAgg = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);


    hashAgg->AddChild(project2);
    project2->AddChild(filter3);
    filter3->AddChild(join1);
    join1->AddChild(scan_part);
    join1->AddChild(project1);
    project1->AddChild(filter2);
    filter2->AddChild(filter1);
    filter1->AddChild(scan_lineitem);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    hashAgg->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q19执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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