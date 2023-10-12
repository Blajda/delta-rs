//! Logical Operations for DataFusion

use datafusion_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
pub const METRIC_OBSERVER_NAME: &str = "MetricObserver";

#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) struct MetricObserver {
    // This acts as an anchor when converting a to physical operator
    pub anchor: String,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for MetricObserver {
    fn name(&self) -> &str {
        METRIC_OBSERVER_NAME
    }

    fn inputs(&self) -> Vec<&datafusion_expr::LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MetricObserver name={}", &self.anchor)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion_expr::Expr],
        inputs: &[datafusion_expr::LogicalPlan],
    ) -> Self {
        MetricObserver {
            anchor: self.anchor.clone(),
            input: inputs[0].clone(),
        }
    }
}
