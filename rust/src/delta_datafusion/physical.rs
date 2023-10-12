//! Physical Operations for DataFusion
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::{
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

pub(crate) type MetricObserverFunction = fn(&RecordBatch, &ExecutionPlanMetricsSet) -> ();

pub(crate) struct MetricObserverExec {
    parent: Arc<dyn ExecutionPlan>,
    anchor: String,
    metrics: ExecutionPlanMetricsSet,
    update: MetricObserverFunction,
}

impl MetricObserverExec {
    pub fn new(anchor: String, parent: Arc<dyn ExecutionPlan>, f: MetricObserverFunction) -> Self {
        MetricObserverExec {
            parent,
            anchor,
            metrics: ExecutionPlanMetricsSet::new(),
            update: f,
        }
    }

    pub fn anchor(&self) -> &str {
        &self.anchor
    }
}

impl std::fmt::Debug for MetricObserverExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricObserverExec")
            .field("anchor", &self.anchor)
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl DisplayAs for MetricObserverExec {
    fn fmt_as(
        &self,
        _: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "MetricObserverExec anchor={}", self.anchor)
    }
}

impl ExecutionPlan for MetricObserverExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.parent.schema()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        self.parent.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[datafusion_physical_expr::PhysicalSortExpr]> {
        self.parent.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.parent.clone()]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let res = self.parent.execute(partition, context)?;
        Ok(Box::pin(MetricObserverStream {
            schema: self.schema(),
            input: res,
            metrics: self.metrics.clone(),
            update: self.update,
        }))
    }

    fn statistics(&self) -> datafusion_common::Statistics {
        self.parent.statistics()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        //TODO: Error on multiple children
        Ok(Arc::new(MetricObserverExec::new(
            self.anchor.clone(),
            children.get(0).unwrap().clone(),
            self.update,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct MetricObserverStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metrics: ExecutionPlanMetricsSet,
    update: MetricObserverFunction,
}

impl Stream for MetricObserverStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                (self.update)(&batch, &self.metrics);
                Some(Ok(batch))
            }
            other => other,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for MetricObserverStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
