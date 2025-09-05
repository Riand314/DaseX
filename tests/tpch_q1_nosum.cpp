#include "import_data.hpp"
#include "file_dir_config.hpp"

// ================================================================TPC-H测试================================================================
// =============================Q1=============================
// 测试Q1,4线程
TEST(TPCHTest, Q1NoSumSingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    // 构建Scan
    std::vector<int> projection_ids = {4, 5 ,6 ,7, 8, 9, 10};
    std::shared_ptr<PhysicalTableScan> scan = std::make_shared<PhysicalTableScan>(table_lineitem, -1, projection_ids);

    // 构建Filter
    std::shared_ptr<BoundColumnRefExpression> l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 6, 0);
    Value value_p(875635200);
    std::shared_ptr<BoundConstantExpression> constant_col = std::make_shared<BoundConstantExpression>(value_p);
    std::shared_ptr<BoundComparisonExpression> comparison_col = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, l_shipdate, constant_col);
    std::shared_ptr<PhysicalFilter> filter = std::make_shared<PhysicalFilter>(comparison_col);

    // 构建Project
    std::shared_ptr<BoundColumnRefExpression> l_returnflag = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> l_linestatus = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> l_tax = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 3, 0);
    Value value_1(1.0f);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<LogicalType> arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function1 = GetSubFunction();
    ScalarProjectFunction bound_function1("-", arguments1, LogicalType::FLOAT, function1, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions1 = {constant1_col, l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function1, expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";
    std::vector<LogicalType> arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function2 = GetMulFunction();
    ScalarProjectFunction bound_function2("*", arguments2, LogicalType::FLOAT, function2, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function2, expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice*(1-l_discount)";

    std::vector<LogicalType> arguments3 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function3 = GetAddFunction();
    ScalarProjectFunction bound_function3("+", arguments3, LogicalType::FLOAT, function3, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions3 = {constant1_col, l_tax};
    std::shared_ptr<BoundProjectFunctionExpression> tax_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function3, expressions3, nullptr);
    tax_1_col->alias = "1+l_tax";

    std::vector<LogicalType> arguments4 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function4 = GetMulFunction();
    ScalarProjectFunction bound_function4("*", arguments4, LogicalType::FLOAT, function4, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions4 = {extendedprice_discount_1_col, tax_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_tax_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function4, expressions4, nullptr);
    extendedprice_tax_col->alias = "l_extendedprice*(1-l_discount)*(1+l_tax)";

    std::vector<std::shared_ptr<Expression>> expressions5 = {l_returnflag, l_linestatus, l_quantity, l_extendedprice, extendedprice_discount_1_col, extendedprice_tax_col, l_discount};
    std::shared_ptr<PhysicalProject> project = std::make_shared<PhysicalProject>(expressions5);

    // 构建HashAgg
    std::vector<int> group_set = {0, 1};
    std::vector<int> agg_set = {2, 3, 4, 5, 2, 3, 6, 2};
    std::vector<int> star_bitmap = {0, 0, 0, 0, 0, 0, 0, 1};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM, AggFunctionType::SUM, AggFunctionType::SUM, AggFunctionType::SUM, AggFunctionType::AVG, AggFunctionType::AVG, AggFunctionType::AVG, AggFunctionType::COUNT};
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);
    hashAgg->exp_name = "1";
    hashAgg->AddChild(project);
    project->AddChild(filter);
    filter->AddChild(scan);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    hashAgg->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q1执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}
// 测试Q1,4线程
TEST(TPCHTest, Q1NoSumFourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(175);
    std::shared_ptr<SchedulerRead> scheduler2 = std::make_shared<SchedulerRead>(175);
    Util::print_socket_free_memory();
    bool importFinish = false;
    std::string fileDir = fileDir4;
    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    // std::vector<int> &work_ids, std::vector<int> &partition_idxs, std::vector<std::string> &file_names
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<std::string> file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        file_names.emplace_back(file_name);
    }
    // InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, file_names, importFinish);
    InsertLineitemMul2(table_lineitem, scheduler2, work_ids, partition_idxs, file_names, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);
    Util::print_socket_free_memory();
    // 构建Scan
    std::vector<int> projection_ids = {4, 5 ,6 ,7, 8, 9, 10};
    std::shared_ptr<PhysicalTableScan> scan = std::make_shared<PhysicalTableScan>(table_lineitem, -1, projection_ids);

    // 构建Filter
    std::shared_ptr<BoundColumnRefExpression> l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 6, 0);
    Value value_p(875635200);
    std::shared_ptr<BoundConstantExpression> constant_col = std::make_shared<BoundConstantExpression>(value_p);
    std::shared_ptr<BoundComparisonExpression> comparison_col = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, l_shipdate, constant_col);
    std::shared_ptr<PhysicalFilter> filter = std::make_shared<PhysicalFilter>(comparison_col);

    // 构建Project
    std::shared_ptr<BoundColumnRefExpression> l_returnflag = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> l_linestatus = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> l_tax = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 3, 0);
    Value value_1(1.0f);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<LogicalType> arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function1 = GetSubFunction();
    ScalarProjectFunction bound_function1("-", arguments1, LogicalType::FLOAT, function1, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions1 = {constant1_col, l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function1, expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";
    std::vector<LogicalType> arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function2 = GetMulFunction();
    ScalarProjectFunction bound_function2("*", arguments2, LogicalType::FLOAT, function2, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions2 = {l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function2, expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice*(1-l_discount)";

    std::vector<LogicalType> arguments3 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function3 = GetAddFunction();
    ScalarProjectFunction bound_function3("+", arguments3, LogicalType::FLOAT, function3, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions3 = {constant1_col, l_tax};
    std::shared_ptr<BoundProjectFunctionExpression> tax_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function3, expressions3, nullptr);
    tax_1_col->alias = "1+l_tax";

    std::vector<LogicalType> arguments4 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function4 = GetMulFunction();
    ScalarProjectFunction bound_function4("*", arguments4, LogicalType::FLOAT, function4, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions4 = {extendedprice_discount_1_col, tax_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_tax_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function4, expressions4, nullptr);
    extendedprice_tax_col->alias = "l_extendedprice*(1-l_discount)*(1+l_tax)";

    std::vector<std::shared_ptr<Expression>> expressions5 = {l_returnflag, l_linestatus};
    // std::vector<std::shared_ptr<Expression>> expressions5 = {l_returnflag, l_linestatus, l_quantity, l_extendedprice, extendedprice_discount_1_col, extendedprice_tax_col, l_discount};
    std::shared_ptr<PhysicalProject> project = std::make_shared<PhysicalProject>(expressions5);

    std::shared_ptr<PhysicalResultCollector> result_collector = std::make_shared<PhysicalResultCollector>(project);
    result_collector->AddChild(project);
    project->AddChild(filter);
    filter->AddChild(scan);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    result_collector->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q1 nosum 执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
    scheduler->shutdown();
    scheduler2->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}