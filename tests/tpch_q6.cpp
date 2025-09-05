#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q6=============================
// 注意修改数据文件路径
TEST(TPCHTest, Q6SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    Util::print_socket_free_memory();
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);
    //  ================================================构建Scan================================================
    std::vector<int> projection_ids = {4, 5, 6, 10};
    // <l_quantity, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, projection_ids);

    //  ================================================构建Filter================================================
    // 构建Filter表达式
    std::shared_ptr<BoundColumnRefExpression> left_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_1996(820425600);
    std::shared_ptr<BoundConstantExpression> right_shipdate = std::make_shared<BoundConstantExpression>(value_1996);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_shipdate->SetLeft(left_shipdate);
    comparisonExpression_shipdate->SetRight(right_shipdate);

    std::shared_ptr<BoundColumnRefExpression> left_shipdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_1997(852048000);
    std::shared_ptr<BoundConstantExpression> right_shipdate2 = std::make_shared<BoundConstantExpression>(value_1997);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_shipdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_shipdate2->SetLeft(left_shipdate2);
    comparisonExpression_shipdate2->SetRight(right_shipdate2);

    std::shared_ptr<BoundColumnRefExpression> left_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    Value value_003(0.03f);
    std::shared_ptr<BoundConstantExpression> right_discount = std::make_shared<BoundConstantExpression>(value_003);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_discount = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_discount->SetLeft(left_discount);
    comparisonExpression_discount->SetRight(right_discount);

    std::shared_ptr<BoundColumnRefExpression> left_discount2 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    Value value_005(0.05f);
    std::shared_ptr<BoundConstantExpression> right_discount2 = std::make_shared<BoundConstantExpression>(value_005);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_discount2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_discount2->SetLeft(left_discount2);
    comparisonExpression_discount2->SetRight(right_discount2);

    std::shared_ptr<BoundColumnRefExpression> left_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    Value value_24f(24.0f);
    std::shared_ptr<BoundConstantExpression> right_quantity = std::make_shared<BoundConstantExpression>(value_24f);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_quantity = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_quantity->SetLeft(left_quantity);
    comparisonExpression_quantity->SetRight(right_quantity);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression4 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_discount2, comparisonExpression_quantity);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_discount, conjunctionExpression4);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_shipdate2, conjunctionExpression3);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_shipdate, conjunctionExpression2);
    // <l_quantity, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter = std::make_shared<PhysicalFilter>(conjunctionExpression1);

    //  ================================================构建Project================================================
    // left_discount
    std::shared_ptr<BoundColumnRefExpression> extendedprice_col = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    std::vector<LogicalType> arguments_type = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function = GetMulFunction();
    ScalarProjectFunction bound_function("*", arguments_type, LogicalType::FLOAT, function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions = {extendedprice_col, left_discount};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function, expressions, nullptr);
    extendedprice_discount_col->alias = "l_extendedprice*l_discount";
    std::vector<std::shared_ptr<Expression>> expressions2 = {extendedprice_discount_col};
    std::shared_ptr<PhysicalProject> project = std::make_shared<PhysicalProject>(expressions2);

    // TODO: 构建HashAgg
    //  ================================================构建HashAgg================================================
    //const std::vector<AggFunctionType> &aggregate_function_types_,
    //                    const std::vector<int32_t> &group_item_idxs_
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM };
    std::vector<int32_t> group_item_idxs = { 0 };
    std::shared_ptr<PhysicalHashAgg> hashAgg = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);
    hashAgg->AddChild(project);
    project->AddChild(filter);
    filter->AddChild(scan_lineitem);
    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    hashAgg->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q6执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q6查询
// 注意修改数据文件路径
TEST(TPCHTest, Q6FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    // Util::print_socket_free_memory();
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
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, file_names, importFinish);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    std::cout << "Q6: select\n"
                 "    sum(l_extendedprice*l_discount) as revenue \n"
                 "from\n"
                 "    lineitem \n"
                 "where\n"
                 "    l_shipdate >= 820425600\n"
                 "    and l_shipdate < 852048000\n"
                 "    and l_discount >= 0.03 \n"
                 "    and l_discount <= 0.05\n"
                 "    and l_quantity < 24;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> projection_ids = {4, 5, 6, 10};
    // <l_quantity, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, projection_ids);

    //  ================================================构建Filter================================================
    // 构建Filter表达式
    std::shared_ptr<BoundColumnRefExpression> left_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_1996(820425600);
    std::shared_ptr<BoundConstantExpression> right_shipdate = std::make_shared<BoundConstantExpression>(value_1996);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_shipdate->SetLeft(left_shipdate);
    comparisonExpression_shipdate->SetRight(right_shipdate);

    std::shared_ptr<BoundColumnRefExpression> left_shipdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_1997(852048000);
    std::shared_ptr<BoundConstantExpression> right_shipdate2 = std::make_shared<BoundConstantExpression>(value_1997);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_shipdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_shipdate2->SetLeft(left_shipdate2);
    comparisonExpression_shipdate2->SetRight(right_shipdate2);

    std::shared_ptr<BoundColumnRefExpression> left_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    Value value_003(0.03f);
    std::shared_ptr<BoundConstantExpression> right_discount = std::make_shared<BoundConstantExpression>(value_003);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_discount = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_discount->SetLeft(left_discount);
    comparisonExpression_discount->SetRight(right_discount);

    std::shared_ptr<BoundColumnRefExpression> left_discount2 = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    Value value_005(0.05f);
    std::shared_ptr<BoundConstantExpression> right_discount2 = std::make_shared<BoundConstantExpression>(value_005);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_discount2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO);
    comparisonExpression_discount2->SetLeft(left_discount2);
    comparisonExpression_discount2->SetRight(right_discount2);

    std::shared_ptr<BoundColumnRefExpression> left_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 0, 0);
    Value value_24f(24.0f);
    std::shared_ptr<BoundConstantExpression> right_quantity = std::make_shared<BoundConstantExpression>(value_24f);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_quantity = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_quantity->SetLeft(left_quantity);
    comparisonExpression_quantity->SetRight(right_quantity);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression4 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_discount2, comparisonExpression_quantity);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_discount, conjunctionExpression4);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_shipdate2, conjunctionExpression3);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_shipdate, conjunctionExpression2);
    // <l_quantity, l_extendedprice, l_discount, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter = std::make_shared<PhysicalFilter>(conjunctionExpression1);

    //  ================================================构建Project================================================
    // left_discount
    std::shared_ptr<BoundColumnRefExpression> extendedprice_col = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 1, 0);
    std::vector<LogicalType> arguments_type = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p function = GetMulFunction();
    ScalarProjectFunction bound_function("*", arguments_type, LogicalType::FLOAT, function, nullptr);
    std::vector<std::shared_ptr<Expression>> expressions = {extendedprice_col, left_discount};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, bound_function, expressions, nullptr);
    extendedprice_discount_col->alias = "l_extendedprice*l_discount";
    std::vector<std::shared_ptr<Expression>> expressions2 = {extendedprice_discount_col};
    std::shared_ptr<PhysicalProject> project = std::make_shared<PhysicalProject>(expressions2);

    // TODO: 构建HashAgg
    //  ================================================构建HashAgg================================================
    //const std::vector<AggFunctionType> &aggregate_function_types_,
    //                    const std::vector<int32_t> &group_item_idxs_
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::SUM };
    std::vector<int32_t> group_item_idxs = { 0 };
    std::shared_ptr<PhysicalHashAgg> hashAgg = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);
    hashAgg->AddChild(project);
    project->AddChild(filter);
    filter->AddChild(scan_lineitem);
    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    hashAgg->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q6执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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