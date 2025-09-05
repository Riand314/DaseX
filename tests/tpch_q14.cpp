#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q14=============================
// 测试单线程Q14查询
TEST(TPCHTest, Q14SingleThreadTest) {
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
    std::vector<int> lineitem_ids = {1, 5, 6, 10};
    // <l_partkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> part_ids = {0, 4};
    // <p_partkey, p_type>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_809884800(809884800);
    std::shared_ptr<BoundConstantExpression> f1_right_constant = std::make_shared<BoundConstantExpression>(value_809884800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_l_shipdate->SetLeft(f1_l_shipdate);
    comparisonExpression_l_shipdate->SetRight(f1_right_constant);

    std::shared_ptr<BoundColumnRefExpression> f1_l_shipdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_812476800(812476800);
    std::shared_ptr<BoundConstantExpression> f1_right_constant2 = std::make_shared<BoundConstantExpression>(value_812476800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_shipdate2->SetLeft(f1_l_shipdate2);
    comparisonExpression_l_shipdate2->SetRight(f1_right_constant2);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_shipdate, comparisonExpression_l_shipdate2);

    // <l_partkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression_and1);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // < p_partkey, p_type, l_partkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    //  =============================================构建case-when Project=============================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 4, 0);
    std::string like_pattern = "PROMO%";
    Value value(like_pattern);
    std::shared_ptr<BoundConstantExpression> p1_constant_col = std::make_shared<BoundConstantExpression>(value);
    ScalarFunction bound_function = LikeFun::GetLikeFunction();
    std::vector<std::shared_ptr<Expression>> arguments = {p1_p_type, p1_constant_col};
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments,nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);

    std::vector<LogicalType> arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function1 = GetSubFunction();
    ScalarProjectFunction bound_function1("-", arguments1, LogicalType::FLOAT, function1, nullptr);
    Value value_1(1.0F);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<std::shared_ptr<Expression>> expressions1 = {constant1_col, p1_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function1, expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function2 = GetMulFunction();
    ScalarProjectFunction bound_function2("*", arguments2, LogicalType::FLOAT, function2, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {p1_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function2, expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice*(1-l_discount)";

    Value value_0(0.0F);
    std::shared_ptr<BoundConstantExpression> p1_constant0_col = std::make_shared<BoundConstantExpression>(value_0);
    std::shared_ptr<BoundCaseExpression> case_expr1 = std::make_shared<BoundCaseExpression>();
    case_expr1->SetWhenExpr(functionExpression);
    case_expr1->SetThenExpr(extendedprice_discount_1_col);
    case_expr1->SetElseExpr(p1_constant0_col);
    case_expr1->alias = "case-1";

    std::vector<std::shared_ptr<Expression>> p1_expressions = {case_expr1, extendedprice_discount_1_col};
    // <case-1, l_extendedprice*(1-l_discount)>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashAgg================================================
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM, AggFunctionType::SUM };
    std::vector<int32_t> group_item_idxs = { 0, 1 };
    // <Sum(case-1), Sum(l_extendedprice*(1-l_discount))>
    std::shared_ptr<PhysicalHashAgg> hashAgg1 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_case_1 = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_sum_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);
    std::vector<LogicalType> p2_arguments = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p2_function = GetMulFunction();
    ScalarProjectFunction p2_bound_function("*", p2_arguments, LogicalType::DOUBLE, p2_function, nullptr);
    Value value_100d(100.0);
    std::shared_ptr<BoundConstantExpression> p2_constant100d_col = std::make_shared<BoundConstantExpression>(value_100d);
    std::vector<std::shared_ptr<Expression>> p2_expressions1 = {p2_constant100d_col, p2_case_1};
    std::shared_ptr<BoundProjectFunctionExpression> col_1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, p2_bound_function, p2_expressions1, nullptr);
    col_1->alias = "100.00*case_1";

    std::vector<LogicalType> p2_arguments2 = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p2_function2 = GetDivFunction();
    ScalarProjectFunction p2_bound_function2("/", p2_arguments2, LogicalType::DOUBLE, p2_function2, nullptr);
    std::vector<std::shared_ptr<Expression>>p2_expressions2 = {col_1, p2_sum_2};
    std::shared_ptr<BoundProjectFunctionExpression> col_2 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, p2_bound_function2, p2_expressions2, nullptr);
    col_2->alias = "promo_revenue";
    std::vector<std::shared_ptr<Expression>> p2_expressions3 = {col_2};
    // <promo_revenue>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions3);
    project2->exp_name = "2";
    std::shared_ptr<PhysicalResultCollector> result_collector = std::make_shared<PhysicalResultCollector>(project2);
    result_collector->AddChild(project2);
    project2->AddChild(hashAgg1);
    hashAgg1->AddChild(project1);
    project1->AddChild(join1);
    join1->AddChild(scan_part);
    join1->AddChild(filter1);
    filter1->AddChild(scan_lineitem);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    result_collector->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q14执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q14查询
TEST(TPCHTest, Q14FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = "../test_data/4thread";
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

    std::cout << "Q14: select\n"
                 "    100.00 * sum(\n"
                 "    case when p_type like 'PROMO%'\n"
                 "        then l_extendedprice*(1-l_discount)\n"
                 "        else 0\n"
                 "        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\n"
                 "from\n"
                 "    lineitem, part\n"
                 "where\n"
                 "    l_partkey = p_partkey\n"
                 "    and l_shipdate >= 809884800\n"
                 "    and l_shipdate < 812476800" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids = {1, 5, 6, 10};
    // <l_partkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> part_ids = {0, 4};
    // <p_partkey, p_type>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_809884800(809884800);
    std::shared_ptr<BoundConstantExpression> f1_right_constant = std::make_shared<BoundConstantExpression>(value_809884800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_l_shipdate->SetLeft(f1_l_shipdate);
    comparisonExpression_l_shipdate->SetRight(f1_right_constant);

    std::shared_ptr<BoundColumnRefExpression> f1_l_shipdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_812476800(812476800);
    std::shared_ptr<BoundConstantExpression> f1_right_constant2 = std::make_shared<BoundConstantExpression>(value_812476800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_shipdate2->SetLeft(f1_l_shipdate2);
    comparisonExpression_l_shipdate2->SetRight(f1_right_constant2);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_shipdate, comparisonExpression_l_shipdate2);

    // <l_partkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression_and1);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // < p_partkey, p_type, l_partkey, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    //  =============================================构建case-when Project=============================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 4, 0);
    std::string like_pattern = "PROMO%";
    Value value(like_pattern);
    std::shared_ptr<BoundConstantExpression> p1_constant_col = std::make_shared<BoundConstantExpression>(value);
    ScalarFunction bound_function = LikeFun::GetLikeFunction();
    std::vector<std::shared_ptr<Expression>> arguments = {p1_p_type, p1_constant_col};
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments,nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);

    std::vector<LogicalType> arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function1 = GetSubFunction();
    ScalarProjectFunction bound_function1("-", arguments1, LogicalType::FLOAT, function1, nullptr);
    Value value_1(1.0F);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<std::shared_ptr<Expression>> expressions1 = {constant1_col, p1_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function1, expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function2 = GetMulFunction();
    ScalarProjectFunction bound_function2("*", arguments2, LogicalType::FLOAT, function2, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {p1_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function2, expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice*(1-l_discount)";

    Value value_0(0.0F);
    std::shared_ptr<BoundConstantExpression> p1_constant0_col = std::make_shared<BoundConstantExpression>(value_0);
    std::shared_ptr<BoundCaseExpression> case_expr1 = std::make_shared<BoundCaseExpression>();
    case_expr1->SetWhenExpr(functionExpression);
    case_expr1->SetThenExpr(extendedprice_discount_1_col);
    case_expr1->SetElseExpr(p1_constant0_col);
    case_expr1->alias = "case-1";

    std::vector<std::shared_ptr<Expression>> p1_expressions = {case_expr1, extendedprice_discount_1_col};
    // <case-1, l_extendedprice*(1-l_discount)>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashAgg================================================
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM, AggFunctionType::SUM };
    std::vector<int32_t> group_item_idxs = { 0, 1 };
    // <Sum(case-1), Sum(l_extendedprice*(1-l_discount))>
    std::shared_ptr<PhysicalHashAgg> hashAgg1 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_case_1 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_sum_2 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    std::vector<LogicalType> p2_arguments = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p2_function = GetMulFunction();
    ScalarProjectFunction p2_bound_function("*", p2_arguments, LogicalType::FLOAT, p2_function, nullptr);
    Value value_100d(100.0);
    std::shared_ptr<BoundConstantExpression> p2_constant100d_col = std::make_shared<BoundConstantExpression>(value_100d);
    std::vector<std::shared_ptr<Expression>> p2_expressions1 = {p2_constant100d_col, p2_case_1};
    std::shared_ptr<BoundProjectFunctionExpression> col_1 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p2_bound_function, p2_expressions1, nullptr);
    col_1->alias = "100.00*case_1";

    std::vector<LogicalType> p2_arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p2_function2 = GetDivFunction();
    ScalarProjectFunction p2_bound_function2("/", p2_arguments2, LogicalType::FLOAT, p2_function2, nullptr);
    std::vector<std::shared_ptr<Expression>>p2_expressions2 = {col_1, p2_sum_2};
    std::shared_ptr<BoundProjectFunctionExpression> col_2 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p2_bound_function2, p2_expressions2, nullptr);
    col_2->alias = "promo_revenue";
    std::vector<std::shared_ptr<Expression>> p2_expressions3 = {col_2};
    // <promo_revenue>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions3);
    project2->exp_name = "2";
    std::shared_ptr<PhysicalResultCollector> result_collector = std::make_shared<PhysicalResultCollector>(project2);
    result_collector->AddChild(project2);
    project2->AddChild(hashAgg1);
    hashAgg1->AddChild(project1);
    project1->AddChild(join1);
    join1->AddChild(scan_part);
    join1->AddChild(filter1);
    filter1->AddChild(scan_lineitem);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    result_collector->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q14执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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

