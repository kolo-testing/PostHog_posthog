from typing import cast, Optional

from posthog.hogql import ast
from posthog.hogql.parser import parse_select, parse_expr
from posthog.hogql_queries.insights.funnels.base import FunnelBase
from posthog.schema import BreakdownType, BreakdownAttributionType
from posthog.utils import DATERANGE_MAP

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
HUMAN_READABLE_TIMESTAMP_FORMAT = "%-d-%b-%Y"


class FunnelUDF(FunnelBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "uuid" not in self._extra_event_fields:
            self._extra_event_fields.append("uuid")

    def get_step_counts_query(self):
        max_steps = self.context.max_steps
        return self._get_step_counts_query(
            outer_select=[
                *self._get_matching_event_arrays(max_steps),
            ],
            inner_select=[
                *self._get_matching_events(max_steps),
            ],
        )

    def conversion_window_limit(self) -> int:
        return int(
            self.context.funnelWindowInterval * DATERANGE_MAP[self.context.funnelWindowIntervalUnit].total_seconds()
        )

    # This is the function that calls the UDF
    # This is used by both the query itself and the actors query
    def _inner_aggregation_query(self):
        if self.context.funnelsFilter.funnelOrderType == "strict":
            inner_event_query = self._get_inner_event_query_for_udf(
                entity_name="events", skip_step_filter=True, skip_entity_filter=True
            )
        else:
            inner_event_query = self._get_inner_event_query_for_udf(entity_name="events")

        default_breakdown_selector = "[]" if self._query_has_array_breakdown() else "''"

        # stores the steps as an array of integers from 1 to max_steps
        # so if the event could be step_0, step_1 or step_4, it looks like [1,2,0,0,5]

        # Each event is going to be a set of steps or it's going to be a set of exclusions. It can't be both.
        steps = ",".join([f"{i + 1} * step_{i}" for i in range(self.context.max_steps)])

        # this will error if they put in a bad exclusion
        exclusions = ""
        if getattr(self.context.funnelsFilter, "exclusions", None):
            exclusions = "".join([f",-{i + 1} * exclusion_{i}" for i in range(1, self.context.max_steps)])

        if self.context.breakdownType == BreakdownType.COHORT:
            fn = "aggregate_funnel_cohort"
            breakdown_prop = ", prop"
        elif self._query_has_array_breakdown():
            fn = "aggregate_funnel_array"
            breakdown_prop = ""
        else:
            fn = "aggregate_funnel"
            breakdown_prop = ""

        prop_selector = "prop" if self.context.breakdown else default_breakdown_selector
        prop_vals = "groupUniqArray(prop)" if self.context.breakdown else f"[{default_breakdown_selector}]"

        breakdown_attribution_string = f"{self.context.breakdownAttributionType}{f'_{self.context.funnelsFilter.breakdownAttributionValue}' if self.context.breakdownAttributionType == BreakdownAttributionType.STEP else ''}"

        inner_select = parse_select(
            f"""
            SELECT
                arrayJoin({fn}(
                    {self.context.max_steps},
                    {self.conversion_window_limit()},
                    '{breakdown_attribution_string}',
                    '{self.context.funnelsFilter.funnelOrderType}',
                    {prop_vals},
                    arraySort(t -> t.1, groupArray(tuple(toFloat(timestamp), uuid, {prop_selector}, arrayFilter((x) -> x != 0, [{steps}{exclusions}]))))
                )) as af_tuple,
                af_tuple.1 as step_reached,
                af_tuple.2 as breakdown,
                af_tuple.3 as timings,
                af_tuple.4 as matched_events_array,
                aggregation_target
            FROM {{inner_event_query}}
            GROUP BY aggregation_target{breakdown_prop}
            HAVING step_reached >= 0
        """,
            {"inner_event_query": inner_event_query},
        )
        return inner_select

    def get_query(self) -> ast.SelectQuery:
        inner_select = self._inner_aggregation_query()

        step_results = ",".join(
            [f"countIf(ifNull(equals(step_reached, {i}), 0)) AS step_{i+1}" for i in range(self.context.max_steps)]
        )
        step_results2 = ",".join([f"sum(step_{i+1}) AS step_{i+1}" for i in range(self.context.max_steps)])

        conversion_time_arrays = ",".join(
            [
                f"groupArrayIf(timings[{i}], timings[{i}] > 0) AS step_{i}_conversion_times"
                for i in range(1, self.context.max_steps)
            ]
        )

        order_by = ",".join([f"step_{i+1} DESC" for i in reversed(range(self.context.max_steps))])

        other_aggregation = "['Other']" if self._query_has_array_breakdown() else "'Other'"

        use_breakdown_limit = self.context.breakdown and self.context.breakdownType in [
            BreakdownType.PERSON,
            BreakdownType.EVENT,
            BreakdownType.GROUP,
        ]

        final_prop = (
            f"if(row_number < {self.get_breakdown_limit()}, breakdown, {other_aggregation})"
            if use_breakdown_limit
            else "breakdown"
        )

        s = parse_select(
            f"""
            SELECT
                {step_results},
                {conversion_time_arrays},
                rowNumberInBlock() as row_number,
                {final_prop} as final_prop
            FROM
                {{inner_select}}
            GROUP BY breakdown
            ORDER BY {order_by}
        """,
            {"inner_select": inner_select},
        )

        mean_conversion_times = ",".join(
            [
                f"arrayMap(x -> if(isNaN(x), NULL, x), [avgArray(step_{i}_conversion_times)])[1] AS step_{i}_average_conversion_time"
                for i in range(1, self.context.max_steps)
            ]
        )
        median_conversion_times = ",".join(
            [
                f"arrayMap(x -> if(isNaN(x), NULL, x), [medianArray(step_{i}_conversion_times)])[1] AS step_{i}_median_conversion_time"
                for i in range(1, self.context.max_steps)
            ]
        )

        # Weird: unless you reference row_number in this outer block, it doesn't work correctly
        s = parse_select(
            f"""
            SELECT
                {step_results2},
                {mean_conversion_times},
                {median_conversion_times},
                groupArray(row_number) as row_number,
                final_prop
            FROM
                {{s}}
            GROUP BY final_prop
        """,
            {"s": s},
        )

        return cast(ast.SelectQuery, s)

    def _get_funnel_person_step_condition(self) -> ast.Expr:
        actorsQuery, breakdownType = (
            self.context.actorsQuery,
            self.context.breakdownType,
        )
        assert actorsQuery is not None

        funnelStep = actorsQuery.funnelStep
        funnelCustomSteps = actorsQuery.funnelCustomSteps
        funnelStepBreakdown = actorsQuery.funnelStepBreakdown

        conditions: list[ast.Expr] = []

        if funnelCustomSteps:
            # this is an adjustment for how UDF funnels represent steps
            funnelCustomSteps = [x - 1 for x in funnelCustomSteps]
            conditions.append(parse_expr(f"step_reached IN {funnelCustomSteps}"))
        elif funnelStep is not None:
            if funnelStep >= 0:
                conditions.append(parse_expr(f"step_reached >= {funnelStep - 1}"))
            else:
                conditions.append(parse_expr(f"step_reached = {-funnelStep - 2}"))
        else:
            raise ValueError("Missing both funnelStep and funnelCustomSteps")

        if funnelStepBreakdown is not None:
            if isinstance(funnelStepBreakdown, int) and breakdownType != "cohort":
                funnelStepBreakdown = str(funnelStepBreakdown)

            conditions.append(
                parse_expr(
                    "arrayFlatten(array(breakdown)) = arrayFlatten(array({funnelStepBreakdown}))",
                    {"funnelStepBreakdown": ast.Constant(value=funnelStepBreakdown)},
                )
            )

        return ast.And(exprs=conditions)

    def actor_query(
        self,
        extra_fields: Optional[list[str]] = None,
    ) -> ast.SelectQuery:
        inner_select = self._inner_aggregation_query()

        where = self._get_funnel_person_step_condition()

        s = parse_select(
            f"""
            SELECT
                aggregation_target AS actor_id,
                [] AS matching_events
            FROM
                {{inner_select}}
            WHERE
                {{where}}
        """,
            {"inner_select": inner_select, "where": where},
        )

        return s
