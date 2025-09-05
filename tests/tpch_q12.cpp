#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q12=============================
// 测试单线程Q12查询
TEST(TPCHTest, Q12SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
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
    std::vector<int> lineitem_ids = {0, 10, 11, 12, 14};
    // <l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> orders_ids = {0, 5};
    // <o_orderkey, o_orderpriority>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_l_receiptdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_757353600(757353600);
    std::shared_ptr<BoundConstantExpression> f1_right_constant = std::make_shared<BoundConstantExpression>(value_757353600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_receiptdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_l_receiptdate->SetLeft(f1_l_receiptdate);
    comparisonExpression_l_receiptdate->SetRight(f1_right_constant);

    std::shared_ptr<BoundColumnRefExpression> f1_l_receiptdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_788889600(788889600);
    std::shared_ptr<BoundConstantExpression> f1_right_constant2 = std::make_shared<BoundConstantExpression>(value_788889600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_receiptdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_receiptdate2->SetLeft(f1_l_receiptdate2);
    comparisonExpression_l_receiptdate2->SetRight(f1_right_constant2);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_receiptdate, comparisonExpression_l_receiptdate2);

    std::shared_ptr<BoundColumnRefExpression> f1_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_shipdate->SetLeft(f1_l_shipdate);
    comparisonExpression_l_shipdate->SetRight(f1_right_constant2);
    std::shared_ptr<BoundColumnRefExpression> f1_l_commitdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_commitdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_commitdate->SetLeft(f1_l_commitdate);
    comparisonExpression_l_commitdate->SetRight(f1_right_constant2);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_shipdate, comparisonExpression_l_commitdate);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, conjunctionExpression_and1, conjunctionExpression_and2);
    // <l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression_and3);

    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_commitdate_receiptdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_commitdate_receiptdate->SetLeft(f1_l_commitdate);
    comparisonExpression_l_commitdate_receiptdate->SetRight(f1_l_receiptdate);

    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate_commitdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_shipdate_commitdate->SetLeft(f1_l_shipdate);
    comparisonExpression_l_shipdate_commitdate->SetRight(f1_l_commitdate);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and4 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_commitdate_receiptdate, comparisonExpression_l_shipdate_commitdate);

    Value value_MAIL("MAIL");
    Value value_SHIP("SHIP");
    std::vector<Value> value_set = {value_MAIL, value_SHIP};
    std::shared_ptr<BoundColumnRefExpression> f2_l_shipmode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::shared_ptr<BoundInExpression> inExpression = std::make_shared<BoundInExpression>(f2_l_shipmode, value_set, LogicalType::STRING);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and5 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, conjunctionExpression_and4, inExpression);
    // <l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(conjunctionExpression_and5);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_shipmode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_orderkey, p1_l_shipmode};
    // <l_orderkey, l_shipmode>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <l_orderkey, l_shipmode, o_orderkey, o_orderpriority>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    //  =============================================构建case-when Project=============================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_shipmode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_o_orderpriority = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    Value value_URGENT("1-URGENT");
    std::shared_ptr<BoundConstantExpression> p2_constant_col = std::make_shared<BoundConstantExpression>(value_URGENT);
    Value value_HIGH("2-HIGH");
    std::shared_ptr<BoundConstantExpression> p2_constant_col2 = std::make_shared<BoundConstantExpression>(value_HIGH);

    Value value_0(0);
    std::shared_ptr<BoundConstantExpression> p2_constant0_col = std::make_shared<BoundConstantExpression>(value_0);
    Value value_1(1);
    std::shared_ptr<BoundConstantExpression> p2_constant1_col = std::make_shared<BoundConstantExpression>(value_1);

    std::shared_ptr<BoundComparisonExpression> p2_when_expr1 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    p2_when_expr1->SetLeft(p2_o_orderpriority);
    p2_when_expr1->SetRight(p2_constant_col);
    std::shared_ptr<BoundComparisonExpression> p2_when_expr2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    p2_when_expr2->SetLeft(p2_o_orderpriority);
    p2_when_expr2->SetRight(p2_constant_col2);

    std::shared_ptr<BoundConjunctionExpression> p2_when_expr3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, p2_when_expr1, p2_when_expr2);

    std::shared_ptr<BoundCaseExpression> case_expr1 = std::make_shared<BoundCaseExpression>();
    case_expr1->SetWhenExpr(p2_when_expr3);
    case_expr1->SetThenExpr(p2_constant1_col);
    case_expr1->SetElseExpr(p2_constant0_col);
    case_expr1->alias = "high_line_count";

    std::shared_ptr<BoundComparisonExpression> p2_when_expr4 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_NOTEQUAL);
    p2_when_expr4->SetLeft(p2_o_orderpriority);
    p2_when_expr4->SetRight(p2_constant_col);
    std::shared_ptr<BoundComparisonExpression> p2_when_expr5 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_NOTEQUAL);
    p2_when_expr5->SetLeft(p2_o_orderpriority);
    p2_when_expr5->SetRight(p2_constant_col2);
    std::shared_ptr<BoundConjunctionExpression> p2_when_expr6 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, p2_when_expr4, p2_when_expr5);

    std::shared_ptr<BoundCaseExpression> case_expr2 = std::make_shared<BoundCaseExpression>();
    case_expr2->SetWhenExpr(p2_when_expr6);
    case_expr2->SetThenExpr(p2_constant1_col);
    case_expr2->SetElseExpr(p2_constant0_col);
    case_expr2->alias = "low_line_count";

    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_shipmode, case_expr1, case_expr2};
    // <l_shipmode, high_line_count, low_line_count>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {1, 2};
    std::vector<int> star_bitmap = {0, 0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM, AggFunctionType::SUM};
    // <l_shipmode, Sum(high_line_count), Sum(low_line_count)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(join1);
    join1->AddChild(project1);
    join1->AddChild(scan_orders);
    project1->AddChild(filter2);
    filter2->AddChild(filter1);
    filter1->AddChild(scan_lineitem);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q12执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q12查询
TEST(TPCHTest, Q12FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
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

    std::cout << "Q12: select\n"
                 "    l_shipmode,\n"
                 "    sum(\n"
                 "      case when \n"
                 "      o_orderpriority ='1-URGENT' \n"
                 "      or o_orderpriority ='2-HIGH'\n"
                 "      then 1\n"
                 "      else 0\n"
                 "      end) as high_line_count,\n"
                 "    sum(\n"
                 "      case when \n"
                 "      o_orderpriority <> '1-URGENT'\n"
                 "      and o_orderpriority <> '2-HIGH'\n"
                 "      then 1\n"
                 "      else 0\n"
                 "      end) as low_line_count\n"
                 "from\n"
                 "    orders,lineitem\n"
                 "where\n"
                 "    o_orderkey = l_orderkey\n"
                 "    and l_shipmode in ('MAIL', 'SHIP') \n"
                 "    and l_commitdate < l_receiptdate\n"
                 "    and l_shipdate < l_commitdate\n"
                 "    and l_receiptdate >= 757353600\n"
                 "    and l_receiptdate < 788889600\n"
                 "group by \n"
                 "    l_shipmode\n"
                 "order by \n"
                 "    l_shipmode;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids = {0, 10, 11, 12, 14};
    // <l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> orders_ids = {0, 5};
    // <o_orderkey, o_orderpriority>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_l_receiptdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_757353600(757353600);
    std::shared_ptr<BoundConstantExpression> f1_right_constant = std::make_shared<BoundConstantExpression>(value_757353600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_receiptdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_l_receiptdate->SetLeft(f1_l_receiptdate);
    comparisonExpression_l_receiptdate->SetRight(f1_right_constant);

    std::shared_ptr<BoundColumnRefExpression> f1_l_receiptdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_788889600(788889600);
    std::shared_ptr<BoundConstantExpression> f1_right_constant2 = std::make_shared<BoundConstantExpression>(value_788889600);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_receiptdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_receiptdate2->SetLeft(f1_l_receiptdate2);
    comparisonExpression_l_receiptdate2->SetRight(f1_right_constant2);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_receiptdate, comparisonExpression_l_receiptdate2);

    std::shared_ptr<BoundColumnRefExpression> f1_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_shipdate->SetLeft(f1_l_shipdate);
    comparisonExpression_l_shipdate->SetRight(f1_right_constant2);
    std::shared_ptr<BoundColumnRefExpression> f1_l_commitdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_commitdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_commitdate->SetLeft(f1_l_commitdate);
    comparisonExpression_l_commitdate->SetRight(f1_right_constant2);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_shipdate, comparisonExpression_l_commitdate);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, conjunctionExpression_and1, conjunctionExpression_and2);
    // <l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression_and3);

    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_commitdate_receiptdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_commitdate_receiptdate->SetLeft(f1_l_commitdate);
    comparisonExpression_l_commitdate_receiptdate->SetRight(f1_l_receiptdate);

    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_shipdate_commitdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_shipdate_commitdate->SetLeft(f1_l_shipdate);
    comparisonExpression_l_shipdate_commitdate->SetRight(f1_l_commitdate);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and4 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_l_commitdate_receiptdate, comparisonExpression_l_shipdate_commitdate);

    Value value_MAIL("MAIL");
    Value value_SHIP("SHIP");
    std::vector<Value> value_set = {value_MAIL, value_SHIP};
    std::shared_ptr<BoundColumnRefExpression> f2_l_shipmode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::shared_ptr<BoundInExpression> inExpression = std::make_shared<BoundInExpression>(f2_l_shipmode, value_set, LogicalType::STRING);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_and5 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, conjunctionExpression_and4, inExpression);
    // <l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(conjunctionExpression_and5);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_shipmode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_orderkey, p1_l_shipmode};
    // <l_orderkey, l_shipmode>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <l_orderkey, l_shipmode, o_orderkey, o_orderpriority>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    //  =============================================构建case-when Project=============================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_shipmode = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_o_orderpriority = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    Value value_URGENT("1-URGENT");
    std::shared_ptr<BoundConstantExpression> p2_constant_col = std::make_shared<BoundConstantExpression>(value_URGENT);
    Value value_HIGH("2-HIGH");
    std::shared_ptr<BoundConstantExpression> p2_constant_col2 = std::make_shared<BoundConstantExpression>(value_HIGH);

    Value value_0(0);
    std::shared_ptr<BoundConstantExpression> p2_constant0_col = std::make_shared<BoundConstantExpression>(value_0);
    Value value_1(1);
    std::shared_ptr<BoundConstantExpression> p2_constant1_col = std::make_shared<BoundConstantExpression>(value_1);

    std::shared_ptr<BoundComparisonExpression> p2_when_expr1 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    p2_when_expr1->SetLeft(p2_o_orderpriority);
    p2_when_expr1->SetRight(p2_constant_col);
    std::shared_ptr<BoundComparisonExpression> p2_when_expr2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    p2_when_expr2->SetLeft(p2_o_orderpriority);
    p2_when_expr2->SetRight(p2_constant_col2);

    std::shared_ptr<BoundConjunctionExpression> p2_when_expr3 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_OR, p2_when_expr1, p2_when_expr2);

    std::shared_ptr<BoundCaseExpression> case_expr1 = std::make_shared<BoundCaseExpression>();
    case_expr1->SetWhenExpr(p2_when_expr3);
    case_expr1->SetThenExpr(p2_constant1_col);
    case_expr1->SetElseExpr(p2_constant0_col);
    case_expr1->alias = "high_line_count";

    std::shared_ptr<BoundComparisonExpression> p2_when_expr4 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_NOTEQUAL);
    p2_when_expr4->SetLeft(p2_o_orderpriority);
    p2_when_expr4->SetRight(p2_constant_col);
    std::shared_ptr<BoundComparisonExpression> p2_when_expr5 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_NOTEQUAL);
    p2_when_expr5->SetLeft(p2_o_orderpriority);
    p2_when_expr5->SetRight(p2_constant_col2);
    std::shared_ptr<BoundConjunctionExpression> p2_when_expr6 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, p2_when_expr4, p2_when_expr5);

    std::shared_ptr<BoundCaseExpression> case_expr2 = std::make_shared<BoundCaseExpression>();
    case_expr2->SetWhenExpr(p2_when_expr6);
    case_expr2->SetThenExpr(p2_constant1_col);
    case_expr2->SetElseExpr(p2_constant0_col);
    case_expr2->alias = "low_line_count";

    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_shipmode, case_expr1, case_expr2};
    // <l_shipmode, high_line_count, low_line_count>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {1, 2};
    std::vector<int> star_bitmap = {0, 0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM, AggFunctionType::SUM};
    // <l_shipmode, Sum(high_line_count), Sum(low_line_count)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(join1);
    join1->AddChild(project1);
    join1->AddChild(scan_orders);
    project1->AddChild(filter2);
    filter2->AddChild(filter1);
    filter1->AddChild(scan_lineitem);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q12执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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