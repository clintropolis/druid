package org.apache.druid.sql.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.Set;

public class DruidCorrelateUnnestRel extends DruidRel<DruidCorrelateUnnestRel>
{
  public DruidCorrelateUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      PlannerContext plannerContext
  )
  {
    super(cluster, traitSet, plannerContext);
  }

  @Nullable
  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return null;
  }

  @Override
  public DruidCorrelateUnnestRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return null;
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    return null;
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return null;
  }

  @Override
  public DruidCorrelateUnnestRel asDruidConvention()
  {
    return null;
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return null;
  }
}
