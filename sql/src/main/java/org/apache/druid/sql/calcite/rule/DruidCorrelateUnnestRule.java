package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnnestDatasourceRel;
import org.apache.druid.sql.calcite.table.RowSignatures;

public class DruidCorrelateUnnestRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidCorrelateUnnestRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            LogicalCorrelate.class,
            operand(DruidRel.class, any()),
            operand(DruidUnnestDatasourceRel.class, any())
        )
    );

    this.plannerContext = plannerContext;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    LogicalCorrelate logicalCorrelate = call.rel(0);
    DruidQueryRel druidQueryRel = call.rel(1);
    DruidUnnestDatasourceRel unnestDatasourceRel = call.rel(2);

    RowSignature rowSignature = RowSignatures.fromRelDataType(
        logicalCorrelate.getRowType().getFieldNames(),
        logicalCorrelate.getRowType()
    );

    // todo: make new projects with druidQueryRel projects + unnestRel projects shifted

    // todo: is this also a DruidUnnestDatasourceRel or is this another rel, e.g. DruidCorrelateUnnestRel
    DruidUnnestDatasourceRel newRel = new DruidUnnestDatasourceRel(
        unnestDatasourceRel.getUncollect(),
        druidQueryRel,
        unnestDatasourceRel.getUnnestProject(),
        plannerContext
    );

    // build this based on the druidUnnestRel
    final RelBuilder relBuilder =
        call.builder()
            .push(newRel);

    call.transformTo(relBuilder.build());
  }
}
